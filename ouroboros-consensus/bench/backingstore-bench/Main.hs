{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE TupleSections      #-}

module Main (
    main
  , oneWritePer100Reads
  ) where

import           Bench.Commands (BackingStoreInitialiser, Cmd (..), run)
import           Cardano.Slotting.Slot (SlotNo, WithOrigin (..))
import           Control.DeepSeq (NFData (..), rwhnf)
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Data.Map.Diff.Strict (Diff)
import qualified Data.Map.Diff.Strict as Diff
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word (Word64)
import           Ouroboros.Consensus.Ledger.Tables (DiffMK (..), KeysMK (..),
                     ValuesMK)
import           Ouroboros.Consensus.Ledger.Tables.Utils (emptyLedgerTables)
import           Ouroboros.Consensus.Storage.LedgerDB.BackingStore
                     (BackingStorePath (..))
import           Ouroboros.Consensus.Storage.LedgerDB.BackingStore.LMDB
                     (LMDBLimits (..))
import           Ouroboros.Consensus.Storage.LedgerDB.Init
                     (BackingStoreSelector (..), newBackingStoreInitialiser)
import qualified System.Directory as Dir
import           System.FS.API (HasFS (..), SomeHasFS (..))
import           System.FS.API.Types (MountPoint (..), mkFsPath)
import           System.FS.IO (ioHasFS)
import           System.IO.Temp (createTempDirectory,
                     getCanonicalTemporaryDirectory)
import qualified Test.QuickCheck.Monadic as QC.Monadic (run)
import           Test.QuickCheck.Monadic (monadicIO)
import           Test.Tasty (TestTree, testGroup, withResource)
import           Test.Tasty.Bench (Benchmark, bench, bgroup, defaultMain,
                     envWithCleanup, nfAppIO)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.LedgerStateOnlyTables (OTLedgerTables,
                     pattern OTLedgerTables)

{-------------------------------------------------------------------------------
  Main benchmarks
-------------------------------------------------------------------------------}

main :: IO ()
main = defaultMain [bgroup "Bench" [
      tests
    , benchmarks
    ]]

benchmarks :: Benchmark
benchmarks = bgroup "BackingStore" [
      benchCmds "oneWritePer100Reads InMem 10_000" bssInMem $
        oneWritePer100Reads 10_000
    , benchCmds "oneWritePer100Reads LMDB  10_000" bssLMDB $
        oneWritePer100Reads 10_000
    ]

benchCmds :: String -> BackingStoreSelector IO -> [Cmd K V D] -> Benchmark
benchCmds name bss cmds0 =
    envWithCleanup ((,cmds0) <$> setup bss) (eCleanup . fst) $
      \ ~(e, cmds) -> bench name $ nfAppIO (runner e) cmds

runner :: MonadThrow m => Env m ks vs d -> [Cmd ks vs d] -> m ()
runner e cmds = do
  shfs <- eMakeNewSomeHasFS e
  run shfs (eBackingStoreInitialiser e) cmds

{-------------------------------------------------------------------------------
  Auxiliary tests
-------------------------------------------------------------------------------}

tests :: TestTree
tests = testGroup "Auxiliary tests" [
      withResource (setup bssInMem) eCleanup $ \eIO -> bgroup "InMem" [
          testProperty "simpleCopy InMem" $ monadicIO $ do
          e <- QC.Monadic.run eIO
          QC.Monadic.run $ runner e simpleCopy
        ]
    , withResource (setup bssLMDB) eCleanup $ \eIO -> bgroup "LMDB" [
          testProperty "simpleCopy LMDB" $ monadicIO $ do
          e <- QC.Monadic.run eIO
          QC.Monadic.run $ runner e simpleCopy
        ]
    ]

{-------------------------------------------------------------------------------
  Backing store selectors
-------------------------------------------------------------------------------}

bssInMem :: BackingStoreSelector m
bssInMem = InMemoryBackingStore

bssLMDB :: BackingStoreSelector IO
bssLMDB = LMDBBackingStore benchLMDBLimits

benchLMDBLimits :: LMDBLimits
benchLMDBLimits = LMDBLimits
  { lmdbMapSize      = 100 * 1_024 * 1_024
  , lmdbMaxDatabases = 3
  , lmdbMaxReaders   = 32
  }

{-------------------------------------------------------------------------------
  Benchmark scenarios
-------------------------------------------------------------------------------}

-- Concrete types of keys, values and diffs that we use in the benchmarks.
type K = OTLedgerTables Word64 Word64 KeysMK
type V = OTLedgerTables Word64 Word64 ValuesMK
type D = OTLedgerTables Word64 Word64 DiffMK

-- | Perform one write per 100 reads.
--
-- This mimicks the flushing behaviour of the LedgerDB: each applied block
-- incurs a read, and we aggregate diffs for 100 blocks before we flush/write
-- them.
--
-- @
--     oneWritePer100Reads 10_000
--  ==
--     [ BSInitFromValues Origin []
--     , BSWrite 99 [Insert 0 at key 0, ..., Insert 99 at key 99]
--     , BSRead 0
--     ...
--     , BSRead 99
--     , BSWrite 199 [Insert 100 at key 100, ..., Insert 199 at key 199]
--     , BSRead 100
--     ...
--     , BSRead 199
--     ...
--     , BSClose
--     ]
-- @
oneWritePer100Reads :: Int -> [Cmd K V D]
oneWritePer100Reads n = concat [
      [ini]
    , workload
    , [close]
    ]
  where
    ini      = BSInitFromValues Origin emptyLedgerTables
    close    = BSClose

    workload = flip concatMap dat $ \block -> mkWrite block : mkReads block

    -- A write aggregates, for a block, the additions to the ledger state. The
    -- slot number that is used for the write corresponds to the youngest block
    -- (i.e., highest slot number), which is by construction the last entry in
    -- the block.
    mkWrite :: [(SlotNo, Word64)] -> Cmd K V D
    mkWrite block = BSWrite (fst $ last block) $
        mkDiffs $ Diff.fromListInserts [(x,x) | (_sl, x) <- block]

    -- Each value is read once.
    mkReads :: [(SlotNo, Word64)] -> [Cmd K V D]
    mkReads block = [BSRead (mkKey x) | (_sl, x) <- block]

    -- A list of blocks. Each block maps slot numbers to a value. This mapping
    -- indicates that this values is added to the ledger tables at the given
    -- slot number.
    dat :: [[(SlotNo, Word64)]]
    dat = groupsOfN 100 $ zip [0..] [0 .. fromIntegral n - 1]

simpleCopy :: [Cmd K V D]
simpleCopy = [
    BSInitFromValues Origin emptyLedgerTables
  , BSCopy (BackingStorePath $ mkFsPath ["copies", "somecopy"])
  , BSClose
  ]

{-------------------------------------------------------------------------------
  Benchmark scenarios: helpers
-------------------------------------------------------------------------------}

mkKey :: k -> OTLedgerTables k v KeysMK
mkKey = mkKeys . Set.singleton

mkKeys :: Set k -> OTLedgerTables k v KeysMK
mkKeys = OTLedgerTables . KeysMK

mkDiffs :: Diff k v -> OTLedgerTables k v DiffMK
mkDiffs = OTLedgerTables . DiffMK

groupsOfN :: Int -> [a] -> [[a]]
groupsOfN n
    | n <= 0    = error "groupsOfN: n should be positive"
    | otherwise = go
  where
    go :: [a] -> [[a]]
    go [] = []
    go xs = take n xs : groupsOfN n (drop n xs)

{-------------------------------------------------------------------------------
  Set up benchmark environment
-------------------------------------------------------------------------------}

-- | The environment to set up when running benchmarks.
--
-- Benchmarked code is run multiple times within the same environment. However,
-- we don't want (on-disk) state to carry over from one run to the other. For
-- this reason, each benchmark run should intialise a new backing store, and
-- each benchmark run should have a clean directory to do filesystem operations
-- in. 'eBackingStoreInitialiser' provides the former, while 'eMakeNewSomeHasFS'
-- provides the latter.
data Env m ks vs d = Env {
    -- | A method for initialising a backing store.
    eBackingStoreInitialiser :: !(BackingStoreInitialiser m ks vs d)
    -- | Creates a fresh directory, and provides an API to interact with it.
  , eMakeNewSomeHasFS        :: !(m (SomeHasFS m))
    -- | How to clean up the 'Env'.
  , eCleanup                 :: !(m ())
  }

instance NFData (Env m ks vs d) where rnf = rwhnf

-- | Sets up a root temporary directory, and creates an 'Env' for it.
--
-- 'eMakeNewSomeHasFS' creates a new temporary directory under the temporary
-- root, such that each benchmark run has a fresh directory to work in.
-- 'eCleanup' will recursively remove the root temporary directory, erasing all
-- directories created by invocations of 'eMakeNewSomeHasFS'.
setup :: BackingStoreSelector IO -> IO (Env IO K V D)
setup bss = do
  sysTmpDir <- getCanonicalTemporaryDirectory
  benchTmpDir <- createTempDirectory sysTmpDir "bench_backingstore"
  let bsi = newBackingStoreInitialiser mempty bss

  let mkSomeHasFS = do
        tmpDir <- createTempDirectory benchTmpDir "run"
        let hfs = ioHasFS (MountPoint tmpDir)

        createDirectory hfs (mkFsPath ["copies"])

        pure $ SomeHasFS hfs

  pure $ Env {
      eBackingStoreInitialiser = bsi
    , eMakeNewSomeHasFS        = mkSomeHasFS
    , eCleanup                 = Dir.removeDirectoryRecursive benchTmpDir
    }
