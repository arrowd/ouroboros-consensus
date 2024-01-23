{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE StandaloneKindSignatures   #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Ouroboros.Consensus.Storage.LedgerDB.V2.Common (
    -- * LedgerDBEnv
    LedgerDBEnv (..)
  , LedgerDBHandle (..)
  , LedgerDBState (..)
  , closeAllForkers
  , getEnv
  , getEnv2
  , getEnv5
  , getEnvSTM
  , getEnvSTM1
    -- * Forkers
  , newForkerAtFromTip
  , newForkerAtPoint
  , newForkerAtTip
  ) where

import           Control.Arrow
import           Control.Tracer
import           Data.Functor.Contravariant ((>$<))
import           Data.Kind
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import           Data.Set (Set)
import           Data.Word
import           GHC.Generics
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Ledger.Tables.Utils
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.V2.LedgerSeq
import           Ouroboros.Consensus.Util
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Prelude hiding (read)
import           System.FS.API

{-------------------------------------------------------------------------------
  The LedgerDBEnv
-------------------------------------------------------------------------------}

type LedgerDBEnv :: (Type -> Type) -> LedgerStateKind -> Type -> Type
data LedgerDBEnv m l blk = LedgerDBEnv {
    -- | INVARIANT: the tip of the 'LedgerDB' is always in sync with the tip of
    -- the current chain of the ChainDB.
    ldbSeq            :: !(StrictTVar m (LedgerSeq m l))
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
  , ldbTracer         :: !(Tracer m (TraceLedgerDBEvent blk))
  , ldbCfg            :: !(LedgerDbCfg l)
  , ldbHasFS          :: !(SomeHasFS m)
  , ldbResolveBlock   :: !(ResolveBlock m blk)
  , ldbSecParam       :: !SecurityParam
  } deriving (Generic)

deriving instance ( IOLike m
                  , LedgerSupportsProtocol blk
                  , NoThunks (l EmptyMK)
                  , NoThunks (Key l)
                  , NoThunks (Value l)
                  , NoThunks (LedgerCfg l)
                  ) => NoThunks (LedgerDBEnv m l blk)

{-------------------------------------------------------------------------------
  The LedgerDBHandle
-------------------------------------------------------------------------------}

type LedgerDBHandle :: (Type -> Type) -> LedgerStateKind -> Type -> Type
newtype LedgerDBHandle m l blk =
    LDBHandle (StrictTVar m (LedgerDBState m l blk))
  deriving Generic

data LedgerDBState m l blk =
    LedgerDBOpen !(LedgerDBEnv m l blk)
  | LedgerDBClosed
  deriving Generic

deriving instance ( IOLike m
                  , LedgerSupportsProtocol blk
                  , NoThunks (l EmptyMK)
                  , NoThunks (Key l)
                  , NoThunks (Value l)
                  , NoThunks (LedgerCfg l)
                  ) => NoThunks (LedgerDBState m l blk)


-- | Check if the LedgerDB is open, if so, executing the given function on the
-- 'LedgerDBEnv', otherwise, throw a 'CloseDBError'.
getEnv ::
     forall m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
  -> (LedgerDBEnv m l blk -> m r)
  -> m r
getEnv (LDBHandle varState) f = readTVarIO varState >>= \case
    LedgerDBOpen env -> f env
    LedgerDBClosed   -> throwIO $ ClosedDBError @blk prettyCallStack

-- | Variant 'of 'getEnv' for functions taking two arguments.
getEnv2 ::
     (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
  -> (LedgerDBEnv m l blk -> a -> b -> m r)
  -> a -> b -> m r
getEnv2 h f a b = getEnv h (\env -> f env a b)

-- | Variant 'of 'getEnv' for functions taking five arguments.
getEnv5 ::
     (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
  -> (LedgerDBEnv m l blk -> a -> b -> c -> d -> e -> m r)
  -> a -> b -> c -> d -> e -> m r
getEnv5 h f a b c d e = getEnv h (\env -> f env a b c d e)

-- | Variant of 'getEnv' that works in 'STM'.
getEnvSTM ::
     forall m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
  -> (LedgerDBEnv m l blk -> STM m r)
  -> STM m r
getEnvSTM (LDBHandle varState) f = readTVar varState >>= \case
    LedgerDBOpen env -> f env
    LedgerDBClosed   -> throwSTM $ ClosedDBError @blk prettyCallStack

-- | Variant of 'getEnv1' that works in 'STM'.
getEnvSTM1 ::
     forall m l blk a r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
  -> (LedgerDBEnv m l blk -> a -> STM m r)
  -> a -> STM m r
getEnvSTM1 (LDBHandle varState) f a = readTVar varState >>= \case
    LedgerDBOpen env -> f env a
    LedgerDBClosed   -> throwSTM $ ClosedDBError @blk prettyCallStack

{-------------------------------------------------------------------------------
  Forkers
-------------------------------------------------------------------------------}

data ForkerEnv m l blk = ForkerEnv {
    -- | Local version of the LedgerSeq
    foeLedgerSeq     :: !(StrictTVar m (LedgerSeq m l))
    -- | This TVar is the same as the LedgerDB one
  , foeSwitchVar     :: !(StrictTVar m (LedgerSeq m l))
    -- | Config
  , foeSecurityParam :: !SecurityParam
    -- | Config
  , foeTracer        :: !(Tracer m TraceForkerEvent)
  }
  deriving Generic

deriving instance ( IOLike m
                  , LedgerSupportsProtocol blk
                  , NoThunks (l EmptyMK)
                  , NoThunks (Key l)
                  , NoThunks (Value l)
                  ) => NoThunks (ForkerEnv m l blk)

getForkerEnv ::
     forall m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
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
  => LedgerDBHandle m l blk
  -> ForkerKey
  -> (ForkerEnv m l blk -> a -> m r)
  -> a -> m r
getForkerEnv1 h forkerKey f a = getForkerEnv h forkerKey (`f` a)

getForkerEnvSTM ::
     forall m l blk r. (IOLike m, HasCallStack, HasHeader blk)
  => LedgerDBHandle m l blk
  -> ForkerKey
  -> (ForkerEnv m l blk -> STM m r)
  -> STM m r
getForkerEnvSTM (LDBHandle varState) forkerKey f = readTVar varState >>= \case
    LedgerDBClosed   -> throwIO $ ClosedDBError @blk prettyCallStack
    LedgerDBOpen env -> readTVar (ldbForkers env) >>= (Map.lookup forkerKey >>> \case
      Nothing        -> throwSTM $ ClosedForkerError @blk prettyCallStack
      Just forkerEnv -> f forkerEnv)

newForker ::
     ( IOLike m
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     , NoThunks (l EmptyMK)
     , GetTip l
     )
  => LedgerDBHandle m l blk
  -> LedgerDBEnv m l blk
  -> LedgerSeq m l
  -> m (Forker m l blk)
newForker h ldbEnv lseq = do
  lseqVar <- newTVarIO lseq
  forkerKey <- atomically $ stateTVar (ldbNextForkerKey ldbEnv) $ \r -> (r, succ r)
  let forkerEnv = ForkerEnv {
      foeLedgerSeq      = lseqVar
    , foeSwitchVar      = ldbSeq ldbEnv
    , foeSecurityParam  = ldbSecParam ldbEnv
    , foeTracer         = LedgerDBForkerEvent . TraceForkerEventWithKey forkerKey >$< ldbTracer ldbEnv
    }
  atomically $ modifyTVar (ldbForkers ldbEnv) $ Map.insert forkerKey forkerEnv
  pure $ mkForker h forkerKey

mkForker ::
     ( IOLike m
     , HasHeader blk
     , HasLedgerTables l
     , GetTip l
     )
  => LedgerDBHandle m l blk
  -> ForkerKey
  -> Forker m l blk
mkForker h forkerKey = Forker {
      forkerClose                  = implForkerClose h forkerKey
    , forkerReadTables             = getForkerEnv1   h forkerKey implForkerReadTables
    , forkerRangeReadTables        = getForkerEnv1   h forkerKey implForkerRangeReadTables
    , forkerRangeReadTablesDefault = getForkerEnv1   h forkerKey implForkerRangeReadTablesDefault
    , forkerGetLedgerState         = getForkerEnvSTM h forkerKey implForkerGetLedgerState
    , forkerReadStatistics         = getForkerEnv    h forkerKey implForkerReadStatistics
    , forkerPush                   = getForkerEnv1   h forkerKey implForkerPush
    , forkerCommit                 = getForkerEnvSTM h forkerKey implForkerCommit
    }

implForkerClose ::
     MonadSTM m
  => LedgerDBHandle m l blk
  -> ForkerKey
  -> m ()
implForkerClose (LDBHandle varState) forkerKey = do
    menv <- atomically $ readTVar varState >>= \case
      LedgerDBClosed       -> pure Nothing
      LedgerDBOpen ldbEnv -> do
        stateTVar
            (ldbForkers ldbEnv)
            (Map.updateLookupWithKey (\_ _ -> Nothing) forkerKey)
    whenJust menv (\e -> traceWith (foeTracer e) ForkerClose)

implForkerReadTables ::
     (MonadSTM m, GetTip l)
  => ForkerEnv m l blk
  -> LedgerTables l KeysMK
  -> m (LedgerTables l ValuesMK)
implForkerReadTables env ks = do
    traceWith (foeTracer env) ForkerReadTablesStart
    ldb <- readTVarIO $ foeLedgerSeq env
    tbs <- read (tables $ currentHandle ldb) ks
    traceWith (foeTracer env) ForkerReadTablesEnd
    pure tbs

implForkerRangeReadTables ::
     (MonadSTM m, GetTip l, HasLedgerTables l)
  => ForkerEnv m l blk
  -> RangeQuery l
  -> m (LedgerTables l ValuesMK)
implForkerRangeReadTables _env _rq0 = undefined -- TODO (js) -- do
    -- traceWith (foeTracer env) ForkerRangeReadTablesStart
    -- ldb <- readTVarIO $ foeLedgerSeq env
    -- case rqPrev rq0 of
    --   Nothing -> readRange (tables $ currentHandle ldb) undefined
    --   Just (LedgerTables (KeysMK ks)) -> do
    --     LedgerTables (ValuesMK m) <- readRange (tables $ currentHandle ldb) undefined
    --     let tbs = LedgerTables $ ValuesMK (maybe m (snd . flip Map.split m) (Set.lookupMax ks))
    --     traceWith (foeTracer env) ForkerRangeReadTablesEnd
    --     pure tbs

implForkerRangeReadTablesDefault ::
     (MonadSTM m, HasLedgerTables l, GetTip l)
  => ForkerEnv m l blk
  -> Maybe (LedgerTables l KeysMK)
  -> m (LedgerTables l ValuesMK)
implForkerRangeReadTablesDefault env prev =
    implForkerRangeReadTables env (RangeQuery prev n)
  where
    n = undefined -- defaultQueryBatchSize $ foeQueryBatchSize env

implForkerGetLedgerState ::
     (MonadSTM m, GetTip l)
  => ForkerEnv m l blk
  -> STM m (l EmptyMK)
implForkerGetLedgerState env = current <$> readTVar (foeLedgerSeq env)

implForkerReadStatistics ::
     (MonadSTM m, GetTip l)
  => ForkerEnv m l blk
  -> m (Maybe Statistics)
implForkerReadStatistics env = do
  traceWith (foeTracer env) ForkerReadStatistics
  ldb <- readTVarIO $ foeLedgerSeq env
  Just . Statistics <$> tablesSize (tables $ currentHandle ldb)

implForkerPush ::
     (MonadSTM m, GetTip l, HasLedgerTables l)
  => ForkerEnv m l blk
  -> l DiffMK
  -> m ()
implForkerPush env newState = do
  traceWith (foeTracer env) ForkerPushStart
  db <- readTVarIO (foeLedgerSeq env)
  let (st, tbs) = (forgetLedgerTables newState, ltprj newState)
  newtbs <- duplicate (tables $ currentHandle db)
  write newtbs tbs
  let db' = prune (foeSecurityParam env)
          $ extend (StateRef st newtbs) db
  atomically $ writeTVar (foeLedgerSeq env) db'
  traceWith (foeTracer env) ForkerPushEnd

implForkerCommit ::
     (MonadSTM m)
  => ForkerEnv m l blk
  -> STM m ()
implForkerCommit env = do
  db <- readTVar (foeLedgerSeq env)
  modifyTVar (foeSwitchVar env) (const db)

{-------------------------------------------------------------------------------
  Acquiring consistent views
-------------------------------------------------------------------------------}

-- Acquire both a value handle and a db changelog at the tip. Holds a read lock
-- while doing so.
acquireAtTip ::
     IOLike m
  => LedgerDBEnv m l blk
  -> m (LedgerSeq m l)
acquireAtTip ldbEnv = readTVarIO (ldbSeq ldbEnv)

-- Acquire both a value handle and a db changelog at the requested point. Holds
-- a read lock while doing so.
acquireAtPoint ::
     forall m l blk. (
       HeaderHash l ~ HeaderHash blk
     , IOLike m
     , IsLedger l
     , StandardHash l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBEnv m l blk
  -> Point blk
  -> m (Either GetForkerError (LedgerSeq m l))
acquireAtPoint ldbEnv pt = do
      dblog <- readTVarIO (ldbSeq ldbEnv)
      let immTip = castPoint $ getTip $ anchor dblog
      case rollback pt dblog of
        Nothing     | pt < immTip -> pure $ Left PointTooOld
                    | otherwise   -> pure $ Left PointNotOnChain
        Just dblog' -> pure $ Right dblog'

-- Acquire both a value handle and a db changelog at n blocks before the tip.
-- Holds a read lock while doing so.
acquireAtFromTip ::
     forall m l blk. (
       IOLike m
     , IsLedger l
     )
  => LedgerDBEnv m l blk
  -> Word64
  -> m (Either ExceededRollback (LedgerSeq m l))
acquireAtFromTip ldbEnv n = do
      dblog <- readTVarIO (ldbSeq ldbEnv)
      case rollbackN n dblog of
        Nothing ->
          return $ Left $ ExceededRollback {
              rollbackMaximum   = maxRollback dblog
            , rollbackRequested = n
            }
        Just dblog' -> pure $ Right dblog'

newForkerAtTip ::
     ( IOLike m
     , IsLedger l
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBHandle m l blk
  -> m (Forker m l blk)
newForkerAtTip h = getEnv h $ \ldbEnv -> do
    acquireAtTip ldbEnv >>= newForker h ldbEnv

newForkerAtPoint ::
     ( HeaderHash l ~ HeaderHash blk
     , IOLike m
     , IsLedger l
     , StandardHash l
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBHandle m l blk
  -> Point blk
  -> m (Either GetForkerError (Forker m l blk))
newForkerAtPoint h pt = getEnv h $ \ldbEnv -> do
    acquireAtPoint ldbEnv pt >>= traverse (newForker h ldbEnv)

newForkerAtFromTip ::
     ( IOLike m
     , IsLedger l
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBHandle m l blk
  -> Word64
  -> m (Either ExceededRollback (Forker m l blk))
newForkerAtFromTip h n = getEnv h $ \ldbEnv -> do
    acquireAtFromTip ldbEnv n >>= traverse (newForker h ldbEnv)

-- | Close all open block and header 'Follower's.
closeAllForkers ::
     MonadSTM m
  => LedgerDBEnv m l blk
  -> m ()
closeAllForkers ldbEnv =
    atomically $ writeTVar forkersVar Map.empty
  where
    forkersVar = ldbForkers ldbEnv
