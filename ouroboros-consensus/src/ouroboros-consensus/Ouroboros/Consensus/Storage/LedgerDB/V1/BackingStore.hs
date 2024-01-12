{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | See "Ouroboros.Consensus.Storage.LedgerDB.BackingStore.API" for the
-- documentation. This module just puts together the implementations for the
-- API, currently two:
--
-- * "Ouroboros.Consensus.Storage.LedgerDB.BackingStore.Impl.InMemory": a @TVar@
--   holding a "Data.Map".
--
-- * "Ouroboros.Consensus.Storage.LedgerDB.BackingStore.Impl.LMDB": an external
--   disk-based database.
module Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore (
    -- * API
    --
    -- | Most of the documentation on the behaviour of the 'BackingStore' lives
    -- in this module.
    module Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.API
    -- * Initialization
  , newBackingStore
  , restoreBackingStore
    -- * Tracing
  , FlavorImplSpecificTrace (..)
    -- * Testing
  , newBackingStoreInitialiser
  ) where

import           Cardano.Slotting.Slot
import           Control.Tracer
import           Data.Functor.Contravariant
import           Data.SOP.Dict
import           GHC.Stack (HasCallStack)
import           Ouroboros.Consensus.Ledger.Basics
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Args
import           Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.API
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.Impl.InMemory as InMemory
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.Impl.LMDB as LMDB
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.Singletons
import           System.FS.API
import           System.FS.API.Types

type BackingStoreInitializer m l =
     SomeHasFS m
  -> InitFrom (LedgerTables l ValuesMK)
  -> m (LedgerBackingStore m l)

-- | Overwrite the 'BackingStore' tables with the snapshot's tables
restoreBackingStore ::
     ( IOLike m
     , HasLedgerTables l
     , CanSerializeLedgerTables l
     , HasCallStack
     , SingI impl
     )
  => Tracer m (FlavorImplSpecificTrace FlavorV1 impl)
  -> BackingStoreArgs impl m
  -> SomeHasFS m
  -> FsPath
  -> m (LedgerBackingStore m l)
restoreBackingStore trcr bss someHasFs loadPath =
    newBackingStoreInitialiser trcr bss someHasFs (InitFromCopy loadPath)

-- | Create a 'BackingStore' from the given initial tables.
newBackingStore ::
     ( IOLike m
     , HasLedgerTables l
     , CanSerializeLedgerTables l
     , HasCallStack
     , SingI impl
     )
  => Tracer m (FlavorImplSpecificTrace FlavorV1 impl)
  -> BackingStoreArgs impl m
  -> SomeHasFS m
  -> LedgerTables l ValuesMK
  -> m (LedgerBackingStore m l)
newBackingStore trcr bss someHasFS tables =
    newBackingStoreInitialiser trcr bss someHasFS (InitFromValues Origin tables)

newBackingStoreInitialiser ::
     forall m l impl.
     ( IOLike m
     , HasLedgerTables l
     , CanSerializeLedgerTables l
     , HasCallStack
     , SingI impl
     )
  => Tracer m (FlavorImplSpecificTrace FlavorV1 impl)
  -> BackingStoreArgs impl m
  -> BackingStoreInitializer m l
newBackingStoreInitialiser trcr bss =
  case (sing :: Sing impl, bss) of
    (SOnDisk, LMDBBackingStoreArgs limits Dict) ->
      LMDB.newLMDBBackingStore
        (OnDiskBackingStoreTrace >$< trcr)
        limits
    (SInMemory, InMemoryBackingStoreArgs) ->
      InMemory.newInMemoryBackingStore
        (InMemoryBackingStoreTrace >$< trcr)

{-------------------------------------------------------------------------------
  Tracing
-------------------------------------------------------------------------------}

data instance FlavorImplSpecificTrace FlavorV1 InMemory =
    InMemoryBackingStoreInitialise
  | InMemoryBackingStoreTrace BackingStoreTrace
  deriving (Eq, Show)

data instance FlavorImplSpecificTrace FlavorV1 OnDisk =
    OnDiskBackingStoreInitialise LMDB.LMDBLimits
  | OnDiskBackingStoreTrace BackingStoreTrace
  deriving (Eq, Show)
