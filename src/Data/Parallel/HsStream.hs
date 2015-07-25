{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_HADDOCK show-extensions #-}


module Data.Parallel.HsStream where


import qualified Data.Sequence as S
import Data.Foldable (mapM_, foldlM, foldl)
import Data.Maybe (isJust, fromJust)
import Data.Traversable (Traversable, mapM)
import Control.Concurrent (forkIO, ThreadId, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Exception.Base (catch, AsyncException(ThreadKilled))
import Control.Monad (liftM, when)
import Prelude hiding (id, mapM, mapM_, take, foldl)
--import Prelude (Bool, Either, Int, Maybe(Just, Nothing), ($), Show, Read, Eq, Ord, (*), Monad)

import qualified Control.Concurrent.Chan.Unagi as UQ
import qualified Control.Concurrent.Chan.Unagi.Bounded as BQ 
-- (InChan, OutChan, newChan, readChan, writeChan, tryReadChan, tryRead)


{- ================================================================== -}
{- ============================== DSL =============================== -}
{- ================================================================== -}

---------------------------------------------------------
-- Interfaz de bajo nivel. Compartir variables es malo --
---------------------------------------------------------

deathKill pids io = catch io (\ThreadKilled -> do
--    putStrLn "deathKill"
    mapM_ killThread pids)

-- Basta con un único threadId para luego encadenar el manejo de la señal (el caso interesante es el join)
data S d = S ThreadId (Queue (Maybe (S.Seq d)))

sWrap :: IOEC -> S o -> IO (S o)
sWrap ec s = return s

sUnfold :: (NFData o) => IOEC -> Int -> (i -> (Maybe (o, i))) -> i -> IO (S o)
sUnfold ec n gen i = do
    qo <- newQueue (queueLimit ec)
    tid <- forkIO $ recc qo i
    return $ S tid qo
    where 
        recc qo i = do
            (elems, i') <- genElems n S.empty i
            when (not $ S.null elems) $ 
                writeQueue qo (Just elems)
            if (isJust i') 
                then recc qo (fromJust i')
                else writeQueue qo Nothing
        genElems 0 seq i = return (seq, Just i)
        genElems size seq i = do
            let res = gen i
            case res of
                Just (v, i') -> do
                    _ <- eval v
                    genElems (size-1) (seq S.|> v) i'
                Nothing -> return (seq, Nothing)

sMap :: (NFData o) => IOEC -> Int -> (i -> o) -> S i -> IO (S o)
sMap ec n f (S tid qi) = do
    qo <- newQueue (queueLimit ec)
    mytid <- forkIO $ deathKill [tid] $ recc qi qo S.empty
    return $ S mytid qo
    where 
        recc qi qo acc = do
            (mChunk, nAcc) <- readChunk acc n qi
            case mChunk of 
                Just chunk -> do 
                    vo <- eval $ fmap f chunk
                    writeQueue qo (Just vo)
                    recc qi qo nAcc
                Nothing -> do
                    when (not $ S.null nAcc) $ do
                        vo <- eval $ fmap f nAcc
                        writeQueue qo (Just vo)
                    writeQueue qo Nothing

sFilter   :: IOEC -> Int -> (i -> Bool) -> S i -> IO (S i)
sFilter ec n cond (S tid qi) = do
    qo <- newQueue (queueLimit ec)
    mytid <- forkIO $ deathKill [tid] $ recc qi qo S.empty
    return $ S mytid qo
    where 
        recc qi qo acc = do
            (mChunk, nAcc) <- readChunkFilter cond acc n qi
            case mChunk of 
                Just chunk -> do 
                    writeQueue qo (Just chunk)
                    recc qi qo nAcc
                Nothing -> do
                    when (not $ S.null nAcc) $ do
                        writeQueue qo (Just nAcc)
                    writeQueue qo Nothing

sJoin :: IOEC -> Int -> S i1 -> S i2 -> IO (S (i1, i2))
sJoin ec n (S tid1 qi1) (S tid2 qi2) = do
    qo <- newQueue (queueLimit ec)
    mytid <- forkIO $ deathKill [tid1, tid2] $ recc qi1 qi2 qo S.empty S.empty
    return $ S mytid qo
    where 
        recc qi1 qi2 qo acc1 acc2 = do
            (mChunk1, nAcc1) <- readChunk acc1 n qi1
            (mChunk2, nAcc2) <- readChunk acc2 n qi2
            case (mChunk1, mChunk2) of
                (Just chunk1, Just chunk2) -> do
                    writeQueue qo (Just (S.zip chunk1 chunk2))
                    recc qi1 qi2 qo nAcc1 nAcc2
                (Nothing, Nothing) -> do
                    when ((not $ S.null nAcc1) && (not $ S.null nAcc2)) $
                        writeQueue qo (Just (S.zip nAcc1 nAcc2))
                    writeQueue qo Nothing
                (Nothing, Just chunk2) -> do
                    when (not $ S.null nAcc1) $
                        writeQueue qo (Just (S.zip nAcc1 chunk2))
                    killThread tid2
                    writeQueue qo Nothing
                (Just chunk1, Nothing) -> do
                    when (not $ S.null nAcc2) $
                        writeQueue qo (Just (S.zip chunk1 nAcc2))
                    killThread tid1
                    writeQueue qo Nothing  

sSplit :: IOEC -> Int -> S i -> IO (S i, S i)
sSplit ec n (S tid qi) = do
    -- Estas colas deberían ser infinitas
    qo1 <- newQueue (queueLimit ec)
    qo2 <- newQueue (queueLimit ec)
    mytid <- forkIO $ deathKill [tid] $ recc qi qo1 qo2 S.empty
    myFakeTid <- forkIO $ exceptionHandler mytid 0
    return $ (S myFakeTid qo1, S myFakeTid qo2)
    where 
        exceptionHandler mytid n = 
            catch 
                (do
                    threadDelay 1000000
                    exceptionHandler mytid n) 
                (\ThreadKilled -> do
                    if (n == 0) 
                        then exceptionHandler mytid 1
                        else killThread mytid)
            
        recc qi qo1 qo2 acc = do
        (mChunk, nAcc) <- readChunk acc n qi
        case mChunk of 
            Just chunk -> do 
                -- esto no esta del todo bien, una qo puede bloquear a la otra
                writeQueue qo1 (Just chunk)
                writeQueue qo2 (Just chunk)
                recc qi qo1 qo2 nAcc
            Nothing -> do
                when (not $ S.null nAcc) $ do
                    -- esto no esta del todo bien, una qo puede bloquear a la otra
                    writeQueue qo1 (Just nAcc)
                    writeQueue qo2 (Just nAcc)
                writeQueue qo1 Nothing
                writeQueue qo2 Nothing

sAppend :: IOEC -> Int -> S i -> S i -> IO (S i)
sAppend ec n (S tid1 qi1) (S tid2 qi2) = do
    qo <- newQueue (queueLimit ec)
    mytid <- forkIO $ deathKill [tid1, tid2] $ recc qi1 qi2 qo
    return $ S mytid qo
    where
        recc_ qi qo acc = do
            (mChunk, nAcc) <- readChunk acc n qi
            case mChunk of 
                Just chunk -> do 
                    writeQueue qo (Just chunk)
                    recc_ qi qo nAcc
                Nothing -> return nAcc
        recc qi1 qi2 qo = do
            acc1 <- recc_ qi1 qo S.empty
            acc2 <- recc_ qi2 qo acc1
            when (not $ S.null acc2) $
                writeQueue qo (Just acc2)
            writeQueue qo Nothing    

sUntil :: IOEC -> (c -> i -> c) -> c -> (c -> Bool) -> S i -> IO (S i)
sUntil ec f z until (S tid qi) = do
    qo <- newQueue (queueLimit ec)
    mytid <- forkIO $ deathKill [tid] $ recc qi qo z tid
    return $ S mytid qo
    where 
        recc qi qo acc tid = do
            i <- readQueue qi
            case i of
                Just vi -> do
                    let 
                        (acc', pos, stop) = foldl (\(a, p, cond) v -> 
                            -- esto no esta muy bien ya que recorre todo el arreglo
                            if cond
                                then (a, p, cond)
                                else let a' = f a v
                                         cond' = until a'
                                      in (a', if cond' then p else p + 1, cond')
                            ) (acc, 0, False) vi
                    if stop
                        then do
                            writeQueue qo (Just $ S.take (pos + 1) vi)
                            -- Matar todo
                            killThread tid
                            writeQueue qo Nothing
                        else do
                            writeQueue qo (Just vi)
                            recc qi qo acc' tid
                Nothing -> do
                    writeQueue qo Nothing

--------------------------------------------------------------
-- Interfaz de alto nivel. Evita que se compartan variables --
--------------------------------------------------------------

data Stream s d where
    StWrap     :: s o -> Stream s o

    StUnfold   :: (NFData o) => Int -> (i -> (Maybe (o, i))) -> i -> Stream s o
    
    StMap      :: (NFData o) => Int -> (i -> o) -> Stream s i -> Stream s o
    StFilter   :: Int -> (i -> Bool) -> Stream s i -> Stream s i
    
    StJoin     :: Int -> Stream s i1 -> Stream s i2 -> Stream s (i1, i2)
    
    StSplit    :: Int -> (Stream s a -> Stream s b1) -> (Stream s a -> Stream s b2) -> Stream s a -> Stream s (b1, b2)

    StAppend   :: Int -> Stream s i -> Stream s i -> Stream s i
    
    StUntil    :: (c -> i -> c) -> c -> (c -> Bool) -> Stream s i -> Stream s i

execStream :: IOEC -> Stream S i -> IO (S i)

execStream ec (StWrap s) = sWrap ec s

execStream ec (StUnfold n gen i) = sUnfold ec n gen i

execStream ec (StMap n f st) = sMap ec n f =<< execStream ec st

execStream ec (StFilter n c st) = sFilter ec n c =<< execStream ec st

execStream ec (StJoin n st1 st2) = do
    s1 <- execStream ec st1
    s2 <- execStream ec st2
    sJoin ec n s1 s2

execStream ec (StSplit n f1 f2 st) = do
    s <- execStream ec st
    (s1, s2) <- sSplit ec n s
    s1' <- execStream ec $ f1 (StWrap s1)
    s2' <- execStream ec $ f2 (StWrap s2)
    sJoin ec n s1' s2'

execStream ec (StAppend n st1 st2) = do
    s1 <- execStream ec st1
    s2 <- execStream ec st2
    sAppend ec n s1 s2

execStream ec (StUntil f z until st) = sUntil ec f z until =<< execStream ec st

{- ================================================================== -}
{- ========================= Util Functions ========================= -}
{- ================================================================== -}

-- | Creates a Stream from a List. Requires 'NFData' of its elements in order to fully evaluate them.
stFromList :: (NFData a) => Int -> [a] -> Stream s a
stFromList dim l = StUnfold dim go l
    where
        go [] = Nothing
        go (x:xs) = Just (x, xs)



{- ================================================================== -}
{- ======================= Stream Execution ========================= -}
{- ================================================================== -}

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


eval :: (NFData a) => a -> IO a
eval a = do
    evaluate $ rnf a
    return a

makeChunk :: S.Seq i -> Int -> (Maybe (S.Seq i), S.Seq i)
makeChunk acc n
    | len == n = (Just acc, S.empty)
    | len > n = (Just pre, post)
    | len < n = (Nothing, acc)
    where 
        len = S.length acc
        (pre, post) = S.splitAt n acc

readChunkFilter :: (i -> Bool) -> S.Seq i -> Int -> Queue (Maybe (S.Seq i)) -> IO (Maybe (S.Seq i), S.Seq i)
readChunkFilter cond acc n qi =
    if (S.length acc >= n) then do
        let (chunk, nAcc) = S.splitAt n acc
        return (Just chunk, nAcc)
    else do
        res <- readQueue qi
        case res of 
            Just vi -> do 
                let (mChunk, nacc) = makeChunk (acc S.>< (S.filter cond vi)) n
                case mChunk of
                    Just chunk -> return (Just chunk, nacc)
                    Nothing -> readChunkFilter cond nacc n qi
            Nothing -> return (Nothing, acc)

readChunk :: S.Seq i -> Int -> Queue (Maybe (S.Seq i)) -> IO (Maybe (S.Seq i), S.Seq i)
readChunk = readChunkFilter (const True)
