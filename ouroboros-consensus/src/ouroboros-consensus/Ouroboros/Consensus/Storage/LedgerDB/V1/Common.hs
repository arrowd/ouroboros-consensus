{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE StandaloneKindSignatures   #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE UndecidableInstances       #-}

module Ouroboros.Consensus.Storage.LedgerDB.V1.Common (
    -- * LedgerDB internal state
    LedgerDBEnv (..)
  , LedgerDBHandle (..)
  , LedgerDBState (..)
  , getEnv
  , getEnv1
  , getEnv2
  , getEnv5
  , getEnvSTM
  , getEnvSTM1
    -- * Forkers
  , ForkerEnv (..)
  , getForkerEnv
  , getForkerEnv1
  , getForkerEnvSTM
    -- * Exposed internals for testing purposes
  , TestInternals (..)
  ) where

import           Control.Arrow
import           Control.Tracer
import           Data.Kind
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Set (Set)
import           Data.Word
import           GHC.Generics (Generic)
import           NoThunks.Class
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Storage.LedgerDB.API as API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Args
import           Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore
import           Ouroboros.Consensus.Storage.LedgerDB.V1.DbChangelog
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Lock
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           System.FS.API

{-------------------------------------------------------------------------------
  LedgerDB internal state
-------------------------------------------------------------------------------}

newtype LedgerDBHandle impl m l blk = LDBHandle (StrictTVar m (LedgerDBState impl m l blk))
  deriving Generic

data LedgerDBState impl m l blk =
    LedgerDBOpen !(LedgerDBEnv impl m l blk)
  | LedgerDBClosed
  deriving Generic

deriving instance ( IOLike m
                  , LedgerSupportsProtocol blk
                  , NoThunks (l EmptyMK)
                  , NoThunks (Key l)
                  , NoThunks (Value l)
                  , NoThunks (LedgerCfg l)
                  ) => NoThunks (LedgerDBState impl m l blk)

type LedgerDBEnv :: LedgerDbStorageFlavor -> (Type -> Type) -> LedgerStateKind -> Type -> Type
data LedgerDBEnv impl m l blk = LedgerDBEnv {
    -- | INVARIANT: the tip of the 'LedgerDB' is always in sync with the tip of
    -- the current chain of the ChainDB.
    ldbChangelog      :: !(StrictTVar m (DbChangelog l))
    -- | Handle to the ledger's backing store, containing the parts that grow too
    -- big for in-memory residency
  , ldbBackingStore   :: !(LedgerBackingStore m l)
    -- | The flush lock to the 'BackingStore'. This lock is crucial when it
    -- comes to keeping the data in memory consistent with the data on-disk.
    --
    -- This lock should be held whenever we want to keep a consistent view of
    -- the backing store for some time. In particular we use this:
    --
    -- - when performing a query on the ledger state, we need to hold a
    --   'LocalStateQueryView' which, while live, must maintain a consistent view
    --   of the DB, and therefore we acquire a Read lock.
    --
    -- - when taking a snapshot of the ledger db, we need to prevent others
    --   from altering the backing store at the same time, thus we acquire a
    --   Write lock.
  , ldbLock           :: !(LedgerDBLock m)
    -- | INVARIANT: this set contains only points that are in the
    -- VolatileDB.
    --
    -- INVARIANT: all points on the current chain fragment are in this set.
    --
    -- The VolatileDB might contain invalid blocks, these will not be in
    -- this set.
    --
    -- When a garbage-collection is performed on the VolatileDB, the points
    -- of the blocks eligible for garbage-collection should be removed from
    -- this set.
  , ldbPrevApplied    :: !(StrictTVar m (Set (RealPoint blk)))
    -- | Open forkers.
    --
    -- INVARIANT: a forker is open iff its 'ForkerKey' is in this 'Map.
  , ldbForkers        :: !(StrictTVar m (Map ForkerKey (ForkerEnv m l blk)))
  , ldbNextForkerKey  :: !(StrictTVar m ForkerKey)

  , ldbSnapshotPolicy :: !SnapshotPolicy
  , ldbTracer         :: !(Tracer m (TraceLedgerDBEvent '(FlavorV1, impl) blk))
  , ldbCfg            :: !(LedgerDbCfg l)
  , ldbHasFS          :: !(SomeHasFS m)
  , ldbShouldFlush    :: !(Word64 -> Bool)
  , ldbQueryBatchSize :: !QueryBatchSize
  , ldbResolveBlock   :: !(ResolveBlock m blk)
  , ldbSecParam       :: !SecurityParam
  } deriving (Generic)

deriving instance ( IOLike m
                  , LedgerSupportsProtocol blk
                  , NoThunks (l EmptyMK)
                  , NoThunks (Key l)
                  , NoThunks (Value l)
                  , NoThunks (LedgerCfg l)
                  ) => NoThunks (LedgerDBEnv impl m l blk)

-- | Check if the LedgerDB is open, if so, executing the given function on the
-- 'LedgerDBEnv', otherwise, throw a 'CloseDBError'.
getEnv ::
     forall impl m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> (LedgerDBEnv impl m l blk -> m r)
  -> m r
getEnv (LDBHandle varState) f = readTVarIO varState >>= \case
    LedgerDBOpen env -> f env
    LedgerDBClosed   -> throwIO $ ClosedDBError @blk prettyCallStack

-- | Variant 'of 'getEnv' for functions taking one argument.
getEnv1 ::
     (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> (LedgerDBEnv impl m l blk -> a -> m r)
  -> a -> m r
getEnv1 h f a = getEnv h (`f` a)

-- | Variant 'of 'getEnv' for functions taking two arguments.
getEnv2 ::
     (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> (LedgerDBEnv impl m l blk -> a -> b -> m r)
  -> a -> b -> m r
getEnv2 h f a b = getEnv h (\env -> f env a b)

-- | Variant 'of 'getEnv' for functions taking five arguments.
getEnv5 ::
     (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> (LedgerDBEnv impl m l blk -> a -> b -> c -> d -> e -> m r)
  -> a -> b -> c -> d -> e -> m r
getEnv5 h f a b c d e = getEnv h (\env -> f env a b c d e)

-- | Variant of 'getEnv' that works in 'STM'.
getEnvSTM ::
     forall impl m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> (LedgerDBEnv impl m l blk -> STM m r)
  -> STM m r
getEnvSTM (LDBHandle varState) f = readTVar varState >>= \case
    LedgerDBOpen env -> f env
    LedgerDBClosed   -> throwSTM $ ClosedDBError @blk prettyCallStack

-- | Variant of 'getEnv1' that works in 'STM'.
getEnvSTM1 ::
     forall impl m l blk a r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> (LedgerDBEnv impl m l blk -> a -> STM m r)
  -> a -> STM m r
getEnvSTM1 (LDBHandle varState) f a = readTVar varState >>= \case
    LedgerDBOpen env -> f env a
    LedgerDBClosed   -> throwSTM $ ClosedDBError @blk prettyCallStack

{-------------------------------------------------------------------------------
  Forkers
-------------------------------------------------------------------------------}

data ForkerEnv m l blk = ForkerEnv {
    -- * Local, consistent view of ledger state
    foeBackingStoreValueHandle :: !(LedgerBackingStoreValueHandle m l)
  , foeChangelog               :: !(StrictTVar m (AnchorlessDbChangelog l))
    -- * Communication with the LedgerDB
    -- | Points to 'ldbChangelog'.
  , foeSwitchVar               :: !(StrictTVar m (DbChangelog l))
    -- * Config
  , foeSecurityParam           :: !SecurityParam
  , foeQueryBatchSize          :: !QueryBatchSize
  , foeTracer                  :: !(Tracer m TraceForkerEvent)
  }
  deriving Generic

deriving instance ( IOLike m
                  , LedgerSupportsProtocol blk
                  , NoThunks (l EmptyMK)
                  , NoThunks (Key l)
                  , NoThunks (Value l)
                  ) => NoThunks (ForkerEnv m l blk)

getForkerEnv ::
     forall impl m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> ForkerKey
  -> (ForkerEnv m l blk -> m r)
  -> m r
getForkerEnv (LDBHandle varState) forkerKey f = do
    forkerEnv <- atomically $ readTVar varState >>= \case
      LedgerDBClosed   -> throwIO $ ClosedDBError @blk prettyCallStack
      LedgerDBOpen env -> readTVar (ldbForkers env) >>= (Map.lookup forkerKey >>> \case
        Nothing        -> throwSTM $ ClosedForkerError @blk prettyCallStack
        Just forkerEnv -> pure forkerEnv)

    f forkerEnv

getForkerEnv1 ::
     (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> ForkerKey
  -> (ForkerEnv m l blk -> a -> m r)
  -> a -> m r
getForkerEnv1 h forkerKey f a = getForkerEnv h forkerKey (`f` a)

getForkerEnvSTM ::
     forall impl m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle impl m l blk
  -> ForkerKey
  -> (ForkerEnv m l blk -> STM m r)
  -> STM m r
getForkerEnvSTM (LDBHandle varState) forkerKey f = readTVar varState >>= \case
    LedgerDBClosed   -> throwIO $ ClosedDBError @blk prettyCallStack
    LedgerDBOpen env -> readTVar (ldbForkers env) >>= (Map.lookup forkerKey >>> \case
      Nothing        -> throwSTM $ ClosedForkerError @blk prettyCallStack
      Just forkerEnv -> f forkerEnv)

{-------------------------------------------------------------------------------
  Exposed internals for testing purposes
-------------------------------------------------------------------------------}

-- TODO: fill in as required
type TestInternals :: (Type -> Type) -> LedgerStateKind -> Type -> Type
data TestInternals m l blk = TestInternals
