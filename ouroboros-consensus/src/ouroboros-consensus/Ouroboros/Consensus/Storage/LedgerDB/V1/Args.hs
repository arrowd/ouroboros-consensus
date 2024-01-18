{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE DeriveAnyClass           #-}
{-# LANGUAGE DeriveGeneric            #-}
{-# LANGUAGE DerivingStrategies       #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE NumericUnderscores       #-}
{-# LANGUAGE PolyKinds                #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Ouroboros.Consensus.Storage.LedgerDB.V1.Args (
    BackingStoreArgs (..)
  , FlushFrequency (..)
  , HasBackingStoreArgs (..)
  , LedgerDbFlavorArgs (..)
  , QueryBatchSize (..)
  , defaultQueryBatchSize
  , defaultShouldFlush
  ) where

import           Control.Monad.IO.Class
import           Data.Kind
import           Data.SOP.Dict
import           Data.Word
import           GHC.Generics
import           NoThunks.Class
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.Impl.LMDB
import           Ouroboros.Consensus.Util.Singletons

{-------------------------------------------------------------------------------
  Arguments
-------------------------------------------------------------------------------}

-- | The /maximum/ number of keys to read in a backing store range query.
--
-- When performing a ledger state query that involves on-disk parts of the
-- ledger state, we might have to read ranges of key-value pair data (e.g.,
-- UTxO) from disk using backing store range queries. Instead of reading all
-- data in one go, we read it in batches. 'QueryBatchSize' determines the size
-- of these batches.
--
-- INVARIANT: Should be at least 1.
--
-- It is fine if the result of a range read contains less than this number of
-- keys, but it should never return more.
data QueryBatchSize =
    -- | A default value, which is determined by a specific 'DiskPolicy'. See
    -- 'defaultDiskPolicy' as an example.
    DefaultQueryBatchSize
    -- | A requested value: the number of keys to read from disk in each batch.
  | RequestedQueryBatchSize Word64
  deriving (Show, Eq, Generic)
  deriving anyclass NoThunks

defaultQueryBatchSize :: QueryBatchSize -> Word64
defaultQueryBatchSize requestedQueryBatchSize = case requestedQueryBatchSize of
    RequestedQueryBatchSize value -> value
    DefaultQueryBatchSize         -> 100_000

-- | The number of diffs in the immutable part of the chain that we have to see
-- before we flush the ledger state to disk. See 'onDiskShouldFlush'.
--
-- INVARIANT: Should be at least 0.
data FlushFrequency =
  -- | A default value, which is determined by a specific 'SnapshotPolicy'. See
    -- 'defaultSnapshotPolicy' as an example.
    DefaultFlushFrequency
    -- | A requested value: the number of diffs in the immutable part of the
    -- chain required before flushing.
  | RequestedFlushFrequency Word64
  deriving (Show, Eq, Generic)

defaultShouldFlush :: FlushFrequency -> (Word64 -> Bool)
defaultShouldFlush requestedFlushFrequency = case requestedFlushFrequency of
      RequestedFlushFrequency value -> (>= value)
      DefaultFlushFrequency         -> (>= 100)

instance HasBackingStoreArgs impl m => HasFlavorArgs '(FlavorV1, impl) m where
  data instance LedgerDbFlavorArgs '(FlavorV1, impl) m = V1Args {
      v1FlushFrequency :: FlushFrequency
    , v1QueryBatchSize :: QueryBatchSize
    , v1BackendArgs    :: BackingStoreArgs impl m
  }

  defaultFlavorArgs = V1Args DefaultFlushFrequency DefaultQueryBatchSize defaultBackingStoreArgs

type HasBackingStoreArgs :: LedgerDbStorageFlavor -> (Type -> Type) -> Constraint
class SingI impl => HasBackingStoreArgs impl m where
  data family BackingStoreArgs impl m
  defaultBackingStoreArgs :: BackingStoreArgs impl m

instance MonadIO m => HasBackingStoreArgs OnDisk m where
  data instance BackingStoreArgs OnDisk m =
    LMDBBackingStoreArgs LMDBLimits (Dict MonadIO m)

  defaultBackingStoreArgs = LMDBBackingStoreArgs defaultLMDBLimits Dict

instance HasBackingStoreArgs InMemory m where
  data instance BackingStoreArgs InMemory m = InMemoryBackingStoreArgs

  defaultBackingStoreArgs = InMemoryBackingStoreArgs
