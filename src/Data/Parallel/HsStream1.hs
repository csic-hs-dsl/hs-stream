{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# OPTIONS_HADDOCK show-extensions #-}


module Data.Parallel.HsStream where


import qualified Data.Sequence as S
import Data.Foldable (mapM_, foldlM, foldl)
import Data.Maybe (isJust, fromJust)
import Data.Traversable (Traversable, mapM)
import Control.Concurrent (forkIO, ThreadId, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, takeMVar, swapMVar, newMVar)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Exception.Base (catch, AsyncException(ThreadKilled))
import Control.Monad (liftM, when, void)
import Prelude hiding (id, mapM, mapM_, take, foldl)
--import Prelude (Bool, Either, Int, Maybe(Just, Nothing), ($), Show, Read, Eq, Ord, (*), Monad)

import qualified Control.Concurrent.Chan.Unagi as UQ
import qualified Control.Concurrent.Chan.Unagi.Bounded as BQ 
-- (InChan, OutChan, newChan, readChan, writeChan, tryReadChan, tryRead)


{- ================================================================== -}
{- ============================== DSL =============================== -}
{- ================================================================== -}

-----------------
-- queue
-----------------

data IOEC = IOEC { queueLimit :: Int }

data Queue a = Bounded (BQ.InChan a) (BQ.OutChan a) | Unbounded (UQ.InChan a) (UQ.OutChan a)


readQueue :: Queue a -> IO a
readQueue (Bounded _ outChan) = BQ.readChan outChan
readQueue (Unbounded _ outChan) = UQ.readChan $ outChan

tryReadQueue :: Queue a -> IO (Maybe a)
tryReadQueue (Bounded _ outChan) = do
    (elem, _) <- BQ.tryReadChan outChan
    BQ.tryRead elem
tryReadQueue (Unbounded _ outChan) = do
    (elem, _) <- UQ.tryReadChan outChan
    UQ.tryRead elem

writeQueue :: Queue a -> a -> IO ()
writeQueue (Bounded inChan _) = BQ.writeChan inChan
writeQueue (Unbounded inChan _) = UQ.writeChan inChan


newBQueue :: Int -> IO (Queue a)
newBQueue limit = do
    (inChan, outChan) <- BQ.newChan limit
    return $ Bounded inChan outChan

newUQueue :: IO (Queue a)
newUQueue = do
    (inChan, outChan) <- UQ.newChan
    return $ Unbounded inChan outChan


newQueue = newBQueue

-----------------
-- clases
-----------------

class Publisher pub i o where
    subscribe :: (Subscriber sub i o) => pub i o -> sub i o -> IO ()

class Subscriber sub i o where
    onSubscribe :: (Subscription s) => sub i o -> s -> IO ()
    onNext      :: sub i o -> i -> IO ()
    onComplete  :: sub i o -> IO ()

class Subscription s where
    request :: s -> Int -> IO ()
    cancel  :: s -> IO ()

class (Subscriber pub i o, Publisher pub i o) => Processor pub i o where

-----------------
-- datos del stream
-----------------

data Subscrip = Subscrip {cancelled :: MVar Bool, demand :: MVar Int}

data QData d = DataMsg (Maybe d) | KillMsg
newData a = DataMsg $ Just a

data AnyPub i o = forall pub. Publisher pub i o => AnyPub (pub i o)
data AnySub i o = forall sub. Subscriber sub i o => AnySub (sub i o)
data AnySubscrip = forall subscrip. Subscription subscrip => AnySubscrip subscrip
data AnyProc i o = forall p. Processor p i o => AnyProc (p i o)



data S i o = S {
--    strId        :: ThreadId, 
    inQueue      :: Queue (QData i), 
    subscription :: MVar AnySubscrip, 
    subscribers  :: MVar [AnySub i o]
}


-- ---------------
-- instancias
-- ---------------
instance Subscription Subscrip where
    request s req = do
        old <- takeMVar $ demand s
        putMVar (demand s) (old + req)
    cancel s = void $ swapMVar (cancelled s) True

instance Subscription AnySubscrip where
    request (AnySubscrip s) = request s
    cancel (AnySubscrip s) = cancel s


instance Publisher AnyPub i o where
    subscribe (AnyPub pub) = subscribe pub
    
instance Subscriber AnySub i o where
    onSubscribe (AnySub sub) = onSubscribe sub
    onNext (AnySub sub) = onNext sub
    onComplete (AnySub sub) = onComplete sub
    
instance Publisher AnyProc i o where
    subscribe (AnyProc pub) = subscribe pub
    
instance Subscriber AnyProc i o where
    onSubscribe (AnyProc sub) = onSubscribe sub
    onNext (AnyProc sub) = onNext sub
    onComplete (AnyProc sub) = onComplete sub

instance Processor AnyProc i o where


instance Publisher S i o where
    subscribe pub sub = do
        cancelledMV <- newMVar False
        demandMV <- newMVar (0 :: Int)
        -- guardar el subscriber en el pub
        subs <- takeMVar $ subscribers pub
        putMVar (subscribers pub) $ (AnySub sub):subs
        onSubscribe sub $ Subscrip cancelledMV demandMV

instance Subscriber S i o where
    onSubscribe sub s = do
        -- guardar la subscripción en el sub
        putMVar (subscription sub) $ AnySubscrip s
        return ()
    onNext sub a = do
        -- agregar el dato a la cola del sub
        writeQueue (inQueue sub) $ newData a
        -- restarle 1 al demand de la subscripcion
        subscrip <- readMVar $ subscription sub
        request subscrip $ -1
    onComplete sub = do
        -- cancelar la subscripcion del sub
        subscrip <- readMVar $ subscription sub
        cancel subscrip
    
instance Processor S i o


-----------------
-- operaciones de bajo nivel
-----------------
sMap :: (NFData o) => (i -> o) -> AnyPub i o -> IO (AnyProc i o)
sMap f pub = do
    inQ <- newUQueue
    mySubscription <- newEmptyMVar
    mySubscribers <- newEmptyMVar
    let p = S inQ mySubscription mySubscribers
    subscribe pub p
    _ <- forkIO $ recurse p
    return $ AnyProc p
    
    where recurse p = undefined

