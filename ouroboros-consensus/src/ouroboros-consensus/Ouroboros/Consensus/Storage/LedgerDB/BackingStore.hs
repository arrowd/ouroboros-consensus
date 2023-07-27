{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StandaloneDeriving         #-}

-- | A store for key-value maps that can be extended with deltas.
--
-- When we talk about deltas we mean differences on key-value entries, for
-- example a deletion of a key-value entry or an insertion.
--
-- Its intended use is for storing data structures from the 'LedgerState' and
-- update them with differences produced by executing the Ledger rules.
module Ouroboros.Consensus.Storage.LedgerDB.BackingStore (
    -- * Backing store interface
    BackingStore (..)
  , BackingStorePath (..)
  , BackingStoreValueHandle (..)
  , InitFrom (..)
  , RangeQuery (..)
  , bsRead
  , castBackingStoreValueHandle
  , withBsValueHandle
    -- * Tracing
  , BackingStoreTrace (..)
  , BackingStoreValueHandleTrace (..)
    -- * Statistics
  , Statistics (..)
    -- * Ledger DB wrappers
  , LedgerBackingStore
  , LedgerBackingStore'
  , LedgerBackingStoreValueHandle
  , LedgerBackingStoreValueHandle'
  ) where

import           Cardano.Slotting.Slot (SlotNo, WithOrigin (..))
import           NoThunks.Class (OnlyCheckWhnfNamed (..))
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.Tables
import           Ouroboros.Consensus.Util.IOLike
import qualified System.FS.API as FS
import qualified System.FS.API.Types as FS

{-------------------------------------------------------------------------------
  Backing store interface
-------------------------------------------------------------------------------}

-- | A backing store for a map
data BackingStore m keys values diff = BackingStore {
    -- | Close the backing store
    --
    -- Other methods throw exceptions if called on a closed store. 'bsClose'
    -- itself is idempotent.
    bsClose       :: !(m ())
    -- | Create a persistent copy
    --
    -- Each backing store implementation will offer a way to initialize itself
    -- from such a path.
    --
    -- The destination path must not already exist. After this operation, it
    -- will be a directory.
  , bsCopy        :: !(FS.SomeHasFS m -> BackingStorePath -> m ())
    -- | Open a 'BackingStoreValueHandle' capturing the current value of the
    -- entire database
  , bsValueHandle :: !(m (BackingStoreValueHandle m keys values))
    -- | Apply a valid diff to the contents of the backing store
  , bsWrite       :: !(SlotNo -> diff -> m ())
  }

deriving via OnlyCheckWhnfNamed "BackingStore" (BackingStore m keys values diff)
  instance NoThunks (BackingStore m keys values diff)

data InitFrom values =
    InitFromValues !(WithOrigin SlotNo) !values
  | InitFromCopy !BackingStorePath

newtype BackingStorePath = BackingStorePath FS.FsPath
  deriving stock (Show, Eq, Ord)
  deriving newtype NoThunks

-- | An ephemeral handle to an immutable value of the entire database
--
-- The performance cost is usually minimal unless this handle is held open too
-- long. We expect clients of the BackingStore to not retain handles for a long
-- time.
data BackingStoreValueHandle m keys values = BackingStoreValueHandle {
    -- | At which slot this Value Handle was created
    bsvhAtSlot    :: !(WithOrigin SlotNo)
    -- | Close the handle
    --
    -- Other methods throw exceptions if called on a closed handle. 'bsvhClose'
    -- itself is idempotent.
  , bsvhClose     :: !(m ())
    -- | See 'RangeQuery'
  , bsvhRangeRead :: !(RangeQuery keys -> m values)
    -- | Read the given keys from the handle
    --
    -- Absent keys will merely not be present in the result instead of causing a
    -- failure or an exception.
  , bsvhRead      :: !(keys -> m values)
    -- | Retrieve statistics
  , bsvhStat      :: !(m Statistics)
  }

castBackingStoreValueHandle ::
     Functor m
  => (values -> values')
  -> (keys' -> keys)
  -> BackingStoreValueHandle m keys values
  -> BackingStoreValueHandle m keys' values'
castBackingStoreValueHandle f g bsvh =
  BackingStoreValueHandle {
      bsvhAtSlot
    , bsvhClose
    , bsvhRangeRead = \(RangeQuery prev count) ->
        fmap f . bsvhRangeRead $  RangeQuery (fmap g prev) count
    , bsvhRead = fmap f . bsvhRead . g
    , bsvhStat
    }
  where
    BackingStoreValueHandle {
        bsvhClose
      , bsvhAtSlot
      , bsvhRangeRead
      , bsvhRead
      , bsvhStat
      } = bsvh

data RangeQuery keys = RangeQuery {
      -- | The result of this range query begin at first key that is strictly
      -- greater than the greatest key in 'rqPrev'.
      --
      -- If the given set of keys is 'Just' but contains no keys, then the query
      -- will return no results. (This is the steady-state once a looping range
      -- query reaches the end of the table.)
      rqPrev  :: Maybe keys
      -- | Roughly how many values to read.
      --
      -- The query may return a different number of values than this even if it
      -- has not reached the last key. The only crucial invariant is that the
      -- query only returns an empty map if there are no more keys to read on
      -- disk.
      --
      -- FIXME: #4398 can we satisfy this invariant if we read keys from disk
      -- but all of them were deleted in the changelog?
    , rqCount :: !Int
    }
    deriving stock (Show, Eq)

deriving via OnlyCheckWhnfNamed "BackingStoreValueHandle" (BackingStoreValueHandle m keys values)
  instance NoThunks (BackingStoreValueHandle m keys values)

-- | A combination of 'bsValueHandle' and 'bsvhRead'
bsRead ::
     MonadThrow m
  => BackingStore m keys values diff
  -> keys
  -> m (WithOrigin SlotNo, values)
bsRead store keys = withBsValueHandle store $ \vh -> do
    values <- bsvhRead vh keys
    pure (bsvhAtSlot vh, values)

-- | A 'IOLike.bracket'ed 'bsValueHandle'
withBsValueHandle ::
     MonadThrow m
  => BackingStore m keys values diff
  -> (BackingStoreValueHandle m keys values -> m a)
  -> m a
withBsValueHandle store =
    bracket
      (bsValueHandle store)
      bsvhClose

{-------------------------------------------------------------------------------
  Tracing
-------------------------------------------------------------------------------}

data BackingStoreTrace =
    BSOpening
  | BSOpened                 !(Maybe FS.FsPath)
  | BSInitialisingFromCopy   !FS.FsPath
  | BSInitialisedFromCopy    !FS.FsPath
  | BSInitialisingFromValues !(WithOrigin SlotNo)
  | BSInitialisedFromValues  !(WithOrigin SlotNo)
  | BSClosing
  | BSAlreadyClosed
  | BSClosed
  | BSCopying                !FS.FsPath
  | BSCopied                 !FS.FsPath
  | BSCreatingValueHandle
  | BSValueHandleTrace       !(Maybe Int) !BackingStoreValueHandleTrace
  | BSCreatedValueHandle
  | BSWriting                !SlotNo
  | BSWritten                !(WithOrigin SlotNo) !SlotNo
  deriving (Eq, Show)

data BackingStoreValueHandleTrace =
    BSVHClosing
  | BSVHAlreadyClosed
  | BSVHClosed
  | BSVHRangeReading
  | BSVHRangeRead
  | BSVHReading
  | BSVHRead
  | BSVHStatting
  | BSVHStatted
  deriving (Eq, Show)

{-------------------------------------------------------------------------------
  Statistics
-------------------------------------------------------------------------------}

-- | Statistics for a key-value store.
--
-- Using 'bsvhStat' on a value handle only provides statistics for the on-disk
-- state of a key-value store. Combine this with information from a
-- 'DbChangelog' to obtain statistics about a "logical" state of the key-value
-- store. See 'getStatistics'.
data Statistics = Statistics {
    -- | The last slot number for which key-value pairs were stored.
    --
    -- INVARIANT: the 'sequenceNumber' returned by using 'bsvhStat' on a value
    -- handle should match 'bsvhAtSlot' for that same value handle.
    sequenceNumber :: !(WithOrigin SlotNo)
    -- | The total number of key-value pair entries that are stored.
  , numEntries     :: !Int
  }
  deriving stock (Show, Eq)

{-------------------------------------------------------------------------------
  Ledger DB wrappers
-------------------------------------------------------------------------------}

-- | A handle to the backing store for the ledger tables
type LedgerBackingStore m l = BackingStore m
      (LedgerTables l KeysMK)
      (LedgerTables l ValuesMK)
      (LedgerTables l DiffMK)

-- | A handle to the backing store for the ledger tables
type LedgerBackingStoreValueHandle m l = BackingStoreValueHandle m
      (LedgerTables l KeysMK)
      (LedgerTables l ValuesMK)

type LedgerBackingStoreValueHandle' m blk =
     LedgerBackingStoreValueHandle  m (ExtLedgerState blk)

type LedgerBackingStore' m blk = LedgerBackingStore m (ExtLedgerState blk)