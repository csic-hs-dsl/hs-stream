{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}


module Tests where


import Data.Parallel.HsStream

import Control.DeepSeq (NFData)
import Data.Foldable (toList)
import Data.Typeable
import System.IO.Unsafe (unsafePerformIO)


{- ================================================================== -}
{- ============================= Utils ============================== -}
{- ================================================================== -}

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected result = if (expected == result) 
    then putStrLn " - OK"
    else error $ " - ERROR: '" ++ show expected ++ "' not equals to '" ++ show result ++ "'"



instance (Show a) => Show (Stream S a) where
    show st = show $ unsafePerformIO $ streamToList st

defaultIOEC :: IOEC
defaultIOEC = IOEC 17

streamToList :: Stream S o -> IO [o]
streamToList stream = do
    S _ queue <- execStream defaultIOEC stream
    reducer queue []
    where 
        reducer queue z = do
            optSeq <- readQueue queue
            case optSeq of
                Just seq-> do
                    reducer queue (reverse (toList seq) ++ z)
                Nothing -> do 
                    return (reverse z)


{- ================================================================== -}
{- ============================= Tests ============================== -}
{- ================================================================== -}

elementsMsg :: Int -> String
elementsMsg n 
    | n == 0 = "Empty"
    | n == 1 = "1 element"
    | n > 1  = show n ++ " elements"


testUnfold :: Int -> Int -> IO ()
testUnfold size chuck = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show chuck
    let expected = [1..size] :: [Int]
        stream = stFromList chuck expected
    result <- streamToList stream
    assertEquals expected result

testUnfoldMap :: Int -> (Int, Int) -> IO ()
testUnfoldMap size (chunkUnf, chunkMap) = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show (chunkUnf, chunkMap)
    let list = [1..size] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap chunkMap fun (stFromList chunkUnf list)
    result <- streamToList stream
    assertEquals expected result

testUnfoldJoin :: Int -> (Int, Int, Int) -> IO ()
testUnfoldJoin size (chunkUnf1, chunkUnf2, chunkJoin) = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show (chunkUnf1, chunkUnf2, chunkJoin)
    let list = [1..size] :: [Int]
        expected = zip list list
        stream = StJoin chunkJoin (stFromList chunkUnf1 list) (stFromList chunkUnf2 list)
    result <- streamToList stream
    assertEquals expected result

testUnfoldSplit :: Int -> (Int, Int) -> IO ()
testUnfoldSplit size (chunkUnf, chunkSplit) = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show (chunkUnf, chunkSplit)
    let list = [1..size] :: [Int]
        expected = zip list list
        stream = StSplit chunkSplit (id) (id) (stFromList chunkUnf list)
    result <- streamToList stream
    assertEquals expected result

testUnfoldAppend :: Int -> Int -> ((Int, Int), Int) -> IO ()
testUnfoldAppend sizeL sizeR ((chunkL, chunkR), chunkAppend) = do
    putStr $ "* (" ++ elementsMsg sizeL ++ ", " ++ elementsMsg sizeR ++ ") - Chunck size = " ++ show ((chunkL, chunkR), chunkAppend)
    let left = [1..sizeL] :: [Int]
        right = [sizeL+1..sizeL+sizeR] :: [Int]
        expected = left ++ right
        stream = StAppend chunkAppend (stFromList chunkL left) (stFromList chunkR right)
    result <- streamToList stream
    assertEquals expected result

testUnfoldUntil :: Int -> Int -> IO ()
testUnfoldUntil size chunkUnf = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show chunkUnf
    let list = [1..] :: [Int]
        cant = size
        fun c _ = c + 1
        cond = (== cant)
        z = 0
        expected = take cant list
        stream = StUntil fun z cond (stFromList chunkUnf list)
    result <- streamToList stream
    assertEquals expected result
        
testUnfoldFilter :: Int -> (Int, Int) -> IO ()
testUnfoldFilter size (chunkUnf, chunkFilter) = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show (chunkUnf, chunkFilter)
    let list = [1..size] :: [Int]
        cond = (/= 0) . (`mod` 5)
        expected = filter cond list
        stream = StFilter chunkFilter cond (stFromList chunkUnf list)
    result <- streamToList stream
    assertEquals expected result

testUnfoldSplitUntil1 :: Int -> (Int, Int) -> IO ()
testUnfoldSplitUntil1 size (chunkUnf, chunkSplit) = do
    putStr $ "* (" ++ elementsMsg size ++ ") - Chunck size = " ++ show (chunkUnf, chunkSplit)
    let list = [1..] :: [Int]
        cant = size
        fun c _ = c + 1
        cond = (== cant)
        expected = zip list (take cant list)
        stream = StSplit chunkSplit (id) (StUntil fun 0 cond) (stFromList chunkUnf list)
    result <- streamToList stream
    assertEquals expected result

testAll :: IO ()
testAll = do 
    putStrLn "StUnfold"
    testUnfold 0 1
    testUnfold 0 3
    testUnfold 50 1
    testUnfold 50 5
    testUnfold 50 3
    putStrLn "StUnfold -> StMap"
    testUnfoldMap 0 (1, 1)
    testUnfoldMap 50 (5, 5)
    testUnfoldMap 80 (8, 4)
    testUnfoldMap 80 (4, 8)
    testUnfoldMap 87 (7, 3)
    testUnfoldMap 87 (3, 7)
    putStrLn "StUnfold => StJoin"
    testUnfoldJoin 50 (1, 1, 1)
    testUnfoldJoin 50 (3, 3, 4)
    testUnfoldJoin 50 (3, 7, 4)
    testUnfoldJoin 50 (7, 3, 2)
    putStrLn "StUnfold => StSplit"
    testUnfoldSplit 50 (1, 1)
    testUnfoldSplit 50 (3, 3)
    testUnfoldSplit 50 (3, 7)
    testUnfoldSplit 50 (7, 3)
    putStrLn "StUnfold => StAppend"
    testUnfoldAppend 0 0 ((1, 1), 1)
    testUnfoldAppend 50 0 ((1, 1), 1)
    testUnfoldAppend 0 50 ((1, 1), 1)
    testUnfoldAppend 50 50 ((5, 5), 10)
    testUnfoldAppend 50 50 ((10, 10), 5)
    testUnfoldAppend 50 50 ((5, 10), 5)
    testUnfoldAppend 50 50 ((3, 7), 5)
    testUnfoldAppend 50 50 ((5, 3), 7)
    testUnfoldAppend 49 73 ((5, 3), 7)
    putStrLn "StUnfold -> StUntil"
    testUnfoldUntil 100 1
    testUnfoldUntil 50 5
    testUnfoldUntil 80 8
    testUnfoldUntil 80 4
    testUnfoldUntil 87 7
    testUnfoldUntil 87 3
    putStrLn "StUnfold -> StFilter"
    testUnfoldFilter 0 (1, 1)
    testUnfoldFilter 50 (5, 5)
    testUnfoldFilter 80 (8, 4)
    testUnfoldFilter 80 (4, 8)
    testUnfoldFilter 87 (7, 3)
    testUnfoldFilter 87 (3, 7)
    putStrLn "StUnfold -> StSplit -> StUntil"    
    testUnfoldSplitUntil1 50 (1, 1)
    testUnfoldSplitUntil1 50 (3, 3)
    testUnfoldSplitUntil1 50 (3, 7)
    testUnfoldSplitUntil1 50 (7, 3)


