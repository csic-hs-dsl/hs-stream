{-# LANGUAGE DeriveDataTypeable #-}


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



instance (Show a) => Show (Stream a) where
    show st = show $ unsafePerformIO $ streamToList st

defaultIOEC :: IOEC
defaultIOEC = IOEC 10


streamToList :: Stream o -> IO [o]
streamToList stream = do
    (_, queue) <- execStream defaultIOEC stream
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

-- ----------------------------------------------------------------------------
test01 :: IO ()
test01 = do
    putStr "StUnfold: Empty - Chunck size = 1"
    let expected = [] :: [Int]
        stream = stFromList 1 expected
    result <- streamToList stream
    assertEquals expected result

test02 :: IO ()
test02 = do
    putStr "StUnfold: Empty - Chunck size = 3"
    let expected = [] :: [Int]
        stream = stFromList 3 expected
    result <- streamToList stream
    assertEquals expected result

test03 :: IO ()
test03 = do
    putStr "StUnfold: 50 elements - Chuck size = 1"
    let expected = [1..50] :: [Int]
        stream = stFromList 1 expected
    result <- streamToList stream
    assertEquals expected result

test04 :: IO ()
test04 = do
    putStr "StUnfold: 50 elements - Chuck size = 5"
    let expected = [1..50] :: [Int]
        stream = stFromList 5 expected
    result <- streamToList stream
    assertEquals expected result

test05 :: IO ()
test05 = do
    putStr "StUnfold: 50 elements - Chuck size = 3"
    let expected = [1..50] :: [Int]
        stream = stFromList 3 expected
    result <- streamToList stream
    assertEquals expected result

-- ----------------------------------------------------------------------------
test06 :: IO ()
test06 = do
    putStr "StUnfold -> StMap: Empty - Chunck size = (1, 1)"
    let list = [] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap 1 fun (stFromList 1 list)
    result <- streamToList stream
    assertEquals expected result
    
test07 :: IO ()
test07 = do
    putStr "StUnfold -> StMap: 50 elements - Chunck size = (5, 5)"
    let list = [1..50] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap 5 fun (stFromList 5 list)
    result <- streamToList stream
    assertEquals expected result

test08 :: IO ()
test08 = do
    putStr "StUnfold -> StMap: 80 elements - Chunck size = (8, 4)"
    let list = [1..80] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap 4 fun (stFromList 8 list)
    result <- streamToList stream
    assertEquals expected result

test09 :: IO ()
test09 = do
    putStr "StUnfold -> StMap: 80 elements - Chunck size = (4, 8)"
    let list = [1..80] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap 8 fun (stFromList 4 list)
    result <- streamToList stream
    assertEquals expected result

test10 :: IO ()
test10 = do
    putStr "StUnfold -> StMap: 87 elements - Chunck size = (7, 3)"
    let list = [1..87] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap 3 fun (stFromList 7 list)
    result <- streamToList stream
    assertEquals expected result

test11 :: IO ()
test11 = do
    putStr "StUnfold -> StMap: 87 elements - Chunck size = (3, 7)"
    let list = [1..87] :: [Int]
        fun = (+5) . (*2)
        expected = map fun list
        stream = StMap 7 fun (stFromList 3 list)
    result <- streamToList stream
    assertEquals expected result

-- ----------------------------------------------------------------------------
test12 :: IO ()
test12 = do
    putStr "StUnfold => StJoin: (Empty, Empty) - Chunck size = ((1, 1), 1)"
    let left = [] :: [Int]
        right = [] :: [Int]
        expected = zip left right
        stream = StJoin 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test13 :: IO ()
test13 = do
    putStr "StUnfold => StJoin: (50 elements, Empty) - Chunck size = ((1, 1), 1)"
    let left = [1..50] :: [Int]
        right = [] :: [Int]
        expected = zip left right
        stream = StJoin 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test14 :: IO ()
test14 = do
    putStr "StUnfold => StJoin: (Empty, 50 elements) - Chunck size = ((1, 1), 1)"
    let left = [] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test15 :: IO ()
test15 = do
    putStr "StUnfold => StJoin: (50 elements, 50 elements) - Chunck size = ((1, 1), 1)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test16 :: IO ()
test16 = do
    putStr "StUnfold => StJoin: (50 elements, 50 elements) - Chunck size = ((5, 5), 10)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 10 (stFromList 5 left) (stFromList 5 right)
    result <- streamToList stream
    assertEquals expected result

test17 :: IO ()
test17 = do
    putStr "StUnfold => StJoin: (50 elements, 50 elements) - Chunck size = ((10, 10), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 5 (stFromList 10 left) (stFromList 10 right)
    result <- streamToList stream
    assertEquals expected result

test18 :: IO ()
test18 = do
    putStr "StUnfold => StJoin: (50 elements, 50 elements) - Chunck size = ((5, 10), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 5 (stFromList 5 left) (stFromList 10 right)
    result <- streamToList stream
    assertEquals expected result

test19 :: IO ()
test19 = do
    putStr "StUnfold => StJoin: (50 elements, 50 elements) - Chunck size = ((3, 7), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 5 (stFromList 3 left) (stFromList 7 right)
    result <- streamToList stream
    assertEquals expected result

test20 :: IO ()
test20 = do
    putStr "StUnfold => StJoin: (50 elements, 50 elements) - Chunck size = ((5, 3), 7)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = zip left right
        stream = StJoin 7 (stFromList 5 left) (stFromList 3 right)
    result <- streamToList stream
    assertEquals expected result

test21 :: IO ()
test21 = do
    putStr "StUnfold => StJoin: (49 elements, 73 elements) - Chunck size = ((5, 3), 7)"
    let left = [1..49] :: [Int]
        right = [101..173] :: [Int]
        expected = zip left right
        stream = StJoin 7 (stFromList 5 left) (stFromList 3 right)
    result <- streamToList stream
    assertEquals expected result

-- ----------------------------------------------------------------------------
test22 :: IO ()
test22 = do
    putStr "StUnfold => StConcat: (Empty, Empty) - Chunck size = ((1, 1), 1)"
    let left = [] :: [Int]
        right = [] :: [Int]
        expected = left ++ right
        stream = StConcat 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test23 :: IO ()
test23 = do
    putStr "StUnfold => StConcat: (50 elements, Empty) - Chunck size = ((1, 1), 1)"
    let left = [1..50] :: [Int]
        right = [] :: [Int]
        expected = left ++ right
        stream = StConcat 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test24 :: IO ()
test24 = do
    putStr "StUnfold => StConcat: (Empty, 50 elements) - Chunck size = ((1, 1), 1)"
    let left = [] :: [Int]
        right = [51..100] :: [Int]
        expected = left ++ right
        stream = StConcat 1 (stFromList 1 left) (stFromList 1 right)
    result <- streamToList stream
    assertEquals expected result

test25 :: IO ()
test25 = do
    putStr "StUnfold => StConcat: (50 elements, 50 elements) - Chunck size = ((5, 5), 10)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = left ++ right
        stream = StConcat 10 (stFromList 5 left) (stFromList 5 right)
    result <- streamToList stream
    assertEquals expected result

test26 :: IO ()
test26 = do
    putStr "StUnfold => StConcat: (50 elements, 50 elements) - Chunck size = ((10, 10), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = left ++ right
        stream = StConcat 5 (stFromList 10 left) (stFromList 10 right)
    result <- streamToList stream
    assertEquals expected result

test27 :: IO ()
test27 = do
    putStr "StUnfold => StConcat: (50 elements, 50 elements) - Chunck size = ((5, 10), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = left ++ right
        stream = StConcat 5 (stFromList 5 left) (stFromList 10 right)
    result <- streamToList stream
    assertEquals expected result

test28 :: IO ()
test28 = do
    putStr "StUnfold => StConcat: (50 elements, 50 elements) - Chunck size = ((3, 7), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = left ++ right
        stream = StConcat 5 (stFromList 3 left) (stFromList 7 right)
    result <- streamToList stream
    assertEquals expected result

test29 :: IO ()
test29 = do
    putStr "StUnfold => StConcat: (50 elements, 50 elements) - Chunck size = ((3, 7), 5)"
    let left = [1..50] :: [Int]
        right = [51..100] :: [Int]
        expected = left ++ right
        stream = StConcat 7 (stFromList 5 left) (stFromList 3 right)
    result <- streamToList stream
    assertEquals expected result
    
test30 :: IO ()
test30 = do
    putStr "StUnfold => StConcat: (49 elements, 73 elements) - Chunck size = ((5, 3), 7)"
    let left = [1..49] :: [Int]
        right = [101..173] :: [Int]
        expected = left ++ right
        stream = StConcat 7 (stFromList 5 left) (stFromList 3 right)
    result <- streamToList stream
    assertEquals expected result



allTests :: IO ()
allTests = do 
    test01 >> test02 >> test03 >> test04 >> test05
    test06 >> test07 >> test08 >> test09 >> test10 >> test11
    test12 >> test13 >> test14 >> test15 >> test16 >> test17 >> test18 >> test19 >> test20 >> test21
    test22 >> test23 >> test24 >> test25 >> test26 >> test27 >> test28 >> test29 >> test30

