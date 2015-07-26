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

class Publisher pub where
    subscribe :: (Subscriber sub) => pub a -> sub a -> IO()

class Subscriber sub where
    onSubscribe :: (Subscription s) => sub a -> s -> IO()
    onNext      :: sub a -> a -> IO()
    onComplete  :: sub a -> IO()

class Subscription s where
    request :: s -> Int -> IO()
    cancel  :: s -> IO()

class (Publisher pro, Subscriber pro) => Processor pro

-----------------
-- datos
-----------------

data Subscrip = Subscrip {cancelled :: MVar Bool, demand :: MVar Int}

-----------------
-- instancias
-----------------

instance Publisher S where
    subscribe pub sub = do
        cancelledMV <- newMVar False
        demandMV <- newMVar (0 :: Int)
        -- guardar el subscriber en el pub
        subs <- takeMVar $ subscribers pub
        putMVar (subscribers pub) $ (AnySub sub):subs
        onSubscribe sub $ Subscrip cancelledMV demandMV

instance Subscriber S where
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
    
instance Processor S

instance Subscription Subscrip where
    request s req = do
        old <- takeMVar $ demand s
        putMVar (demand s) (old + req)
    cancel s = void $ swapMVar (cancelled s) True

-----------------
-- datos del stream
-----------------

data QData d = DataMsg (Maybe d) | KillMsg
newData a = DataMsg $ Just a

data AnySub d = forall sub. Subscriber sub => AnySub (sub d)
data AnySubscrip = forall subscrip. Subscription subscrip => AnySubscrip subscrip

instance Subscriber AnySub where
    onSubscribe (AnySub sub) = onSubscribe sub
    onNext (AnySub sub) = onNext sub
    onComplete (AnySub sub) = onComplete sub
    
instance Subscription AnySubscrip where
    request (AnySubscrip s) = request s
    cancel (AnySubscrip s) = cancel s

data S d = S {
    strId :: ThreadId, 
    inQueue :: (Queue (QData d)), 
    subscription :: MVar AnySubscrip, 
    subscribers :: MVar [AnySub d]
}

-----------------
-- stream
-----------------

data Stream s d where
    StWrap     :: s o -> Stream s o

    StUnfold   :: (NFData o) => Int -> (i -> (Maybe (o, i))) -> i -> Stream s o
    
    StMap      :: (NFData o) => Int -> (i -> o) -> Stream s i -> Stream s o
    StFilter   :: Int -> (i -> Bool) -> Stream s i -> Stream s i
    
    StJoin     :: Int -> Stream s i1 -> Stream s i2 -> Stream s (i1, i2)
    
    StSplit    :: Int -> (Stream s a -> Stream s b1) -> (Stream s a -> Stream s b2) -> Stream s a -> Stream s (b1, b2)

    StAppend   :: Int -> Stream s i -> Stream s i -> Stream s i
    
    StUntil    :: (c -> i -> c) -> c -> (c -> Bool) -> Stream s i -> Stream s i
    
