{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE DeriveGeneric            #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE PolyKinds                #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}

{-# OPTIONS_GHC -Wno-orphans #-}
-- |

module Ouroboros.Consensus.Storage.LedgerDB.V1.Args (
    BackingStoreArgs (..)
  , FlushFrequency (..)
  , HasBackingStoreArgs (..)
  , LedgerDbFlavorArgs (..)
  , defaultShouldFlush
  ) where

import           Control.Monad.IO.Class
import           Data.Kind
import           Data.SOP.Dict
import           Data.Word
import           GHC.Generics
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.Impl.LMDB
import           Ouroboros.Consensus.Util.Singletons

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

instance HasBackingStoreArgs impl m => HasFlavorArgs FlavorV1 impl m where
  data instance LedgerDbFlavorArgs FlavorV1 impl m = V1Args {
      v1FlushFrequency :: FlushFrequency
    , v1BackendArgs    :: BackingStoreArgs impl m
  }

  defaultFlavorArgs = V1Args DefaultFlushFrequency defaultBackingStoreArgs
