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
import Control.Monad (liftM, when)
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
stFromList :: (NFData a) => Int -> [a] -> Stream a
stFromList dim l = StUnfold dim go l
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

makeChunk :: S.Seq i -> Int -> (Maybe (S.Seq i), S.Seq i)
makeChunk acc n
    | len == n = (Just acc, S.empty)
    | len > n = (Just pre, post)
    | len < n = (Nothing, acc)
    where 
        len = S.length acc
        (pre, post) = S.splitAt n acc

readChunk :: S.Seq i -> Int -> Queue (Maybe (S.Seq i)) -> IO (Maybe (S.Seq i), S.Seq i)
readChunk acc n qi =
    if (S.length acc >= n) then do
        let (chunk, nAcc) = S.splitAt n acc
        return (Just chunk, nAcc)
    else do
        res <- readQueue qi
        case res of 
            Just vi -> do 
                let (mChunk, nacc) = makeChunk (acc S.>< vi) n
                case mChunk of
                    Just chunk -> return (Just chunk, nacc)
                    Nothing -> readChunk nacc n qi
            Nothing -> return (Nothing, acc)

execStream :: IOEC -> Stream i -> IO (Queue (Maybe (S.Seq i)), Queue BackMsg)
execStream ec (StUnfold n gen i) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    _ <- forkIO $ recc qo bqi i
    return (qo, bqi)
    where 
        recc qo bqi i = do
            backMsg <- tryReadQueue bqi
            case backMsg of
                Nothing -> do
                    (elems, i') <- genElems n S.empty i
                    when (not $ S.null elems) $ 
                        writeQueue qo (Just elems)
                    if (isJust i') 
                        then recc qo bqi (fromJust i')
                        else writeQueue qo Nothing
                Just Stop -> do
                    writeQueue qo Nothing
        genElems 0 seq i = return (seq, Just i)
        genElems size seq i = do
            let res = gen i
            case res of
                Just (v, i') -> do
                    _ <- eval v
                    genElems (size-1) (seq S.|> v) i'
                Nothing -> return (seq, Nothing)


execStream ec (StMap n f stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo S.empty
    return (qo, bqi)
    where 
        recc qi qo bqi bqo acc = do
            let 
                continue = do
                    (mChunk, nAcc) <- readChunk acc n qi
                    case mChunk of 
                        Just chunk -> do 
                            vo <- eval $ fmap f chunk
                            writeQueue qo (Just vo)
                            recc qi qo bqi bqo nAcc
                        Nothing -> do
                            when (not $ S.null nAcc) $ do
                                vo <- eval $ fmap f nAcc
                                writeQueue qo (Just vo)
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo

-- Le falta el stop!!!!!!!!!!!!!!!!!!
execStream ec (StJoin n st1 st2) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi1, bqo1) <- execStream ec st1
    (qi2, bqo2) <- execStream ec st2
    _ <- forkIO $ recc qi1 qi2 qo bqi bqo1 bqo2 S.empty S.empty
    return (qo, bqi)
    where 
        recc qi1 qi2 qo bqi bqo1 bqo2 acc1 acc2 = do
            (mChunk1, nAcc1) <- readChunk acc1 n qi1
            (mChunk2, nAcc2) <- readChunk acc2 n qi2
            case (mChunk1, mChunk2) of
                (Just chunk1, Just chunk2) -> do
                    writeQueue qo (Just (S.zip chunk1 chunk2))
                    recc qi1 qi2 qo bqi bqo1 bqo2 nAcc1 nAcc2
                (Nothing, Nothing) -> do
                    when ((not $ S.null nAcc1) && (not $ S.null nAcc2)) $
                        writeQueue qo (Just (S.zip nAcc1 nAcc2))
                    writeQueue qo Nothing
                (Nothing, Just chunk2) -> do
                    when (not $ S.null nAcc1) $
                        writeQueue qo (Just (S.zip nAcc1 chunk2))
                    writeQueue qo Nothing
                (Just chunk1, Nothing) -> do
                    when (not $ S.null nAcc2) $
                        writeQueue qo (Just (S.zip chunk1 nAcc2))
                    writeQueue qo Nothing                    

execStream ec (StConcat n st1 st2) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi1, bqo1) <- execStream ec st1
    (qi2, bqo2) <- execStream ec st2
    _ <- forkIO $ recc qi1 qi2 qo bqi bqo1 bqo2
    return (qo, bqi)
    where
        recc_ qi qo bqi bqo acc = do
            (mChunk, nAcc) <- readChunk acc n qi
            case mChunk of 
                Just chunk -> do 
                    writeQueue qo (Just chunk)
                    recc_ qi qo bqi bqo nAcc
                Nothing -> return nAcc
        recc qi1 qi2 qo bqi bqo1 bqo2 = do
            acc1 <- recc_ qi1 qo bqi bqo1 S.empty
            acc2 <- recc_ qi2 qo bqi bqo2 acc1
            when (not $ S.null acc2) $
                writeQueue qo (Just acc2)
            writeQueue qo Nothing            

{-
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

-}
