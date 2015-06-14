{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_HADDOCK show-extensions #-}


module HsStream where


import qualified Data.Sequence as S
import Data.Foldable (mapM_, foldlM)
import Data.Maybe (isJust, fromJust)
import Data.Traversable (Traversable, mapM)
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (liftM)
import Prelude hiding (id, mapM, mapM_, take)
--import Prelude (Bool, Either, Int, Maybe(Just, Nothing), ($), Show, Read, Eq, Ord, (*), Monad)

import Control.Concurrent.Chan.Unagi.Bounded (InChan, OutChan, newChan, readChan, writeChan, tryReadChan, tryRead)



{- ================================================================== -}
{- ============================== DSL =============================== -}
{- ================================================================== -}

data Stream d where
    StUnfold   :: (NFData o) => Int -> (i -> (Maybe (o, i))) -> i -> Stream o
    
    StMap      :: (NFData o) => Int -> (i -> o) -> Stream i -> Stream o
    StFilter   :: Int -> (i -> Bool) -> Stream i -> Stream i
    
    StJoin     :: Int -> Stream i1 -> Stream i2 -> Stream (i1, i2)
--    StSplit    :: Int -> Stream i -> (Stream i, Stream i)

    StConcat   :: Int -> Stream i -> Stream i -> Stream i
    
    StUntil    :: (c -> i -> c) -> c -> (c -> Bool) -> Stream i -> Stream i

{- ================================================================== -}
{- ======================= Execution Context ======================== -}
{- ================================================================== -}

{-
class (Monad m) => Exec m where
    type Context m :: *
    type Future m :: * -> *
    exec :: Context m -> Skel (Future m) i o -> i -> m o
-}

{- ================================================================== -}
{- ========================= Util Functions ========================= -}
{- ================================================================== -}

-- | Creates a Stream from a List. Requires 'NFData' of its elements in order to fully evaluate them.
stFromList :: (DIM dim, NFData a) => dim -> [a] -> Stream dim a
stFromList dim l = StUnfoldr dim go l
    where
        go [] = Nothing
        go (x:xs) = Just (x, xs)



{- ================================================================== -}
{- ======================= Stream Execution ========================= -}
{- ================================================================== -}

data IOEC = IOEC { queueLimit :: Int }

data Queue a = Queue { 
    inChan :: InChan a, 
    outChan :: OutChan a
}

newQueue :: Int -> IO (Queue a)
newQueue limit = do
    (inChan, outChan) <- newChan limit
    return $ Queue inChan outChan

readQueue :: Queue a -> IO a
readQueue = readChan . outChan

tryReadQueue :: Queue a -> IO (Maybe a)
tryReadQueue q = do
    elem <- tryReadChan $ outChan q
    tryRead elem

writeQueue :: Queue a -> a -> IO ()
writeQueue = writeChan . inChan

eval :: (NFData a) => a -> IO a
eval a = do
    evaluate $ rnf a
    return a

data BackMsg = Stop

handleBackMsg :: IO () -> Queue BackMsg -> Queue BackMsg -> IO ()
handleBackMsg continue bqi bqo = do
    backMsg <- tryReadQueue bqi
    case backMsg of
        Nothing -> continue
        Just Stop -> writeQueue bqo Stop


execStream :: IOEC -> Stream i -> IO (Queue (Maybe (S.Seq i)), Queue BackMsg)
execStream ec (StUnfoldr dim gen i) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    _ <- forkIO $ recc qo bqi i
    return (qo, bqi)
    where 
        recc qo bqi i = do
            backMsg <- tryReadQueue bqi
            case backMsg of
                Nothing -> do
                    (elems, i') <- genElems (dimLinearSize dim) S.empty i
                    if (not $ S.null elems) 
                        then writeQueue qo (Just elems)
                        else return ()
                    if (isJust i') 
                        then recc qo bqi (fromJust i')
                        else writeQueue qo Nothing
                Just Stop -> do
                    writeQueue qo Nothing
                    return ()
        genElems 0 seq i = return (seq, Just i)
        genElems size seq i = do
            let res = gen i
            case res of
                Just (v, i') -> do
                    _ <- eval v
                    genElems (size-1) (seq S.|> v) i'
                Nothing -> return (seq, Nothing)


execStream ec (StMap _ sk stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo
    return (qo, bqi)
    where 
        recc qi qo bqi bqo = do
            let 
                continue = do
                    res <- readQueue qi
                    case res of 
                        Just vi -> do 
                            vo <- eval $ fmap sk vi
                            writeQueue qo (Just vo)
                            recc qi qo bqi bqo
                        Nothing -> writeQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StChunk dim stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    let chunkSize = dimLinearSize dim
    _ <- forkIO $ recc qi qo bqi bqo S.empty chunkSize
    return (qo, bqi)
    where 
        recc qi qo bqi bqo storage chunkSize = do
            let 
                continue = do
                    i <- readQueue qi
                    case i of
                        Just vi -> do
                            let storage' = storage S.>< vi
                            if (S.length storage' == chunkSize) 
                                then do
                                    writeQueue qo (Just $ storage')
                                    recc qi qo bqi bqo S.empty chunkSize
                                else do
                                    recc qi qo bqi bqo storage' chunkSize
                        Nothing -> do
                            if (S.length storage > 0)
                                then writeQueue qo (Just $ storage)
                                else return ()
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StUnChunk _ stream) = do
    let chunk = dimHead . stDim $ stream
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo chunk
    return (qo, bqi)
    where 
        recc qi qo bqi bqo chunk = do
            let 
                continue = do
                    i <- readQueue qi
                    case i of
                        Just vsi -> do
                            let maxChunkIdx = div (S.length vsi) chunk
                            mapM_ (\c -> writeQueue qo . Just . S.take chunk . S.drop (c * chunk) $ vsi) [0 .. maxChunkIdx]
                            recc qi qo bqi bqo chunk
                        Nothing -> do
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo

{-
execStream ec (StParMap _ sk stream) = do
    qo1 <- newQueue (queueLimit ec)
    bqi1 <- newQueue (queueLimit ec)
    qo2 <- newQueue (queueLimit ec)
    bqi2 <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc1 qi qo1 bqi1 bqo
    _ <- forkIO $ recc2 qo1 qo2 bqi2 bqi1
    return (qo2, bqi2)
    where 
        recc1 qi qo bqi bqo = do
            let 
                continue = do
                    res <- readQueue qi
                    case res of 
                        Just vi -> do
                            vo <- exec ec (SkFork (SkMap sk)) vi
                            writeQueue qo (Just vo)
                            recc1 qi qo bqi bqo
                        Nothing -> writeQueue qo Nothing
            handleBackMsg continue bqi bqo
        recc2 qi qo bqi bqo = do
            let 
                continue = do
                    res <- readQueue qi
                    case res of 
                        Just vi -> do
                            vo <- exec ec SkSync vi
                            writeQueue qo (Just vo)
                            recc2 qi qo bqi bqo
                        Nothing -> writeQueue qo Nothing
            handleBackMsg continue bqi bqo
-}
execStream ec (StUntil _ skF z skCond stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo z
    return (qo, bqi)
    where 
        recc qi qo bqi bqo acc = do
            let 
                continue = do
                    i <- readQueue qi
                    case i of
                        Just vi -> do
                            (acc', pos, stop) <- 
                                foldlM (\(a, p, cond) v -> 
                                    do -- esto no esta muy bien ya que recorre todo el arreglo
                                        if cond
                                            then do
                                                return (a, p, cond)
                                            else do
                                                a' <- return $ skF a v
                                                cond' <- return $ skCond a'
                                                return (a', p + 1, cond')
                                    ) (acc, 0, False) vi
                            if stop
                                then do
                                    writeQueue qo (Just $ S.take (pos + 1) vi)
                                    writeQueue bqo Stop
                                    writeQueue qo Nothing
                                else do
                                    writeQueue qo (Just vi)
                                    recc qi qo bqi bqo acc'
                        Nothing -> do
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo


