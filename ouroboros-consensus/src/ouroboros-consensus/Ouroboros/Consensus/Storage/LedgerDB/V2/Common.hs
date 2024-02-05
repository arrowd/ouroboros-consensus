{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE StandaloneKindSignatures   #-}
{-# LANGUAGE TupleSections              #-}
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
import           Data.Function (on)
import           Data.Functor.Contravariant ((>$<))
import           Data.Kind
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import           Data.Set (Set)
import           Data.Word
import           Debug.Trace (trace)
import           GHC.Generics
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Ledger.Tables.Utils
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Common
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Validate
import           Ouroboros.Consensus.Storage.LedgerDB.V2.LedgerSeq
import           Ouroboros.Consensus.Util
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry
import qualified Ouroboros.Network.AnchoredSeq as AS
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
  , ldbRegistry       :: !(ResourceRegistry m)
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

-- | Forkers live between two resource registries, namely the one that creates
-- it and the LedgerDB one. Whether a forker was committed will decide which
-- registry keeps the reference. The 'StateRef' as will land in the LedgerDB
-- will hold a reference to the 'ResourceRegistry' of the LedgerDB, whereas
-- 'tsrTempKey' will hold a reference for the outer registry (chain selection,
-- local state query server).
data TempStateRef m l = TempStateRef {
    tsrStateRef :: !(StateRef m l)
  , tsrTempKey  :: !(ResourceKey m)
  } deriving Generic

deriving instance (IOLike m, NoThunks (l EmptyMK)) => NoThunks (TempStateRef m l)

instance Eq (l EmptyMK) => Eq (TempStateRef m l) where
  (==) = (==) `on` tsrStateRef

instance Show (l EmptyMK) => Show (TempStateRef m l) where
  show = show . tsrStateRef

instance GetTip l => AS.Anchorable (WithOrigin SlotNo) (StateRef m l) (TempStateRef m l) where
  asAnchor = tsrStateRef
  getAnchorMeasure _ = getTipSlot . state

{-------------------------------------------------------------------------------
  The ForkerLedgerSeq
-------------------------------------------------------------------------------}

-- | A slightly modified 'LedgerSeq' augmented with an additional 'ResourceKey'
-- on the items **that are not the anchor**.
newtype ForkerLedgerSeq m l = ForkerLedgerSeq {
    forkerGetLedgerSeq :: AS.AnchoredSeq (WithOrigin SlotNo) (StateRef m l) (TempStateRef m l)
  } deriving (Generic)

deriving newtype instance (IOLike m, NoThunks (l EmptyMK)) => NoThunks (ForkerLedgerSeq m l)

deriving newtype instance Eq   (l EmptyMK) => Eq   (ForkerLedgerSeq m l)
deriving newtype instance Show (l EmptyMK) => Show (ForkerLedgerSeq m l)

-- | Same as 'currentHandle'.
forkerCurrentHandle :: GetTip l => ForkerLedgerSeq m l -> StateRef m l
forkerCurrentHandle = AS.headAnchor . forkerGetLedgerSeq

-- | Same as 'current'.
forkerCurrent :: GetTip l => ForkerLedgerSeq m l -> l EmptyMK
forkerCurrent = state . forkerCurrentHandle

-- | Same as 'prune'.
forkerPrune ::
     GetTip l
  => SecurityParam
  -> ForkerLedgerSeq m l
  -> ForkerLedgerSeq m l
forkerPrune (SecurityParam k) (ForkerLedgerSeq ldb) =
    ForkerLedgerSeq ldb'
  where
    nvol = AS.length ldb

    ldb' =
      if toEnum nvol <= k
      then ldb
      else snd $ AS.splitAt (nvol - fromEnum k) ldb

-- | Same as 'extend'.
forkerExtend ::
     GetTip l
  => TempStateRef m l
  -> ForkerLedgerSeq m l
  -> ForkerLedgerSeq m l
forkerExtend newState =
  ForkerLedgerSeq . (AS.:> newState) . forkerGetLedgerSeq

{-------------------------------------------------------------------------------
  Forker operations
-------------------------------------------------------------------------------}

data ForkerUsedAfterCommit = ForkerUsedAfterCommit deriving (Show, Exception)

-- | Throw an exception if the forker has been committed before.
guardUncommitted ::
     (MonadSTM m, MonadThrow m)
  => ForkerEnv m l blk
  -> (ForkerLedgerSeq m l -> m a)
  -> m a
guardUncommitted env f = do
  committed <- readTVarIO (foeWasCommitted env)
  if committed
    then trace "USEDAFTERCOMMIT" (throwIO ForkerUsedAfterCommit)
    else f =<< readTVarIO (foeLedgerSeq env)

-- | Throw an exception (in STM) if the forker has been committed before.
guardUncommittedSTM ::
     (MonadSTM m, MonadThrow (STM m))
  => ForkerEnv m l blk
  -> (ForkerLedgerSeq m l -> STM m a)
  -> STM m a
guardUncommittedSTM env f = do
  committed <- readTVar (foeWasCommitted env)
  if committed
    then throwSTM ForkerUsedAfterCommit
    else f =<< readTVar (foeLedgerSeq env)

data ForkerEnv m l blk = ForkerEnv {
    -- | Local version of the LedgerSeq
    foeLedgerSeq          :: !(StrictTVar m (ForkerLedgerSeq m l))
    -- | This TVar is the same as the LedgerDB one
  , foeSwitchVar          :: !(StrictTVar m (LedgerSeq m l))
    -- | Config
  , foeSecurityParam      :: !SecurityParam
    -- | Config
  , foeTracer             :: !(Tracer m TraceForkerEvent)

    -- | Keys here will be removed (forgotten) without running its freer
    -- function.
  , foeResourcesToRemove  :: !(StrictTVar m [ResourceKey m])

    -- | Keys here will be released.
  , foeResourcesToRelease :: !(StrictTVar m [ResourceKey m])

    -- | Signal that this forker was committed.
  , foeWasCommitted       :: !(StrictTVar m Bool)
  }
  deriving Generic

closeForkerEnv :: IOLike m => ForkerEnv m l blk -> m ()
closeForkerEnv e = do
  mapM_ release =<< readTVarIO (foeResourcesToRelease e)
  mapM_ unsafeRemoveResource =<< readTVarIO (foeResourcesToRemove e)
  wasCommitted <- readTVarIO (foeWasCommitted e)
  traceWith (foeTracer e) $
    if wasCommitted
    then ForkerCloseCommitted
    else ForkerCloseUncommitted

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
  -> String
  -> ResourceRegistry m
  -> StateRef m l
  -> m (Forker m l blk)
newForker h ldbEnv why rr st = do
    forkerKey <- atomically $ stateTVar (ldbNextForkerKey ldbEnv) $ \r -> (r, succ r)
    let tr = LedgerDBForkerEvent . TraceForkerEventWithKey forkerKey >$< ldbTracer ldbEnv
    traceWith tr (ForkerOpen why)
    lseqVar   <- newTVarIO . ForkerLedgerSeq . AS.Empty $ st
    toRemove  <- newTVarIO []
    toRelease <- newTVarIO []
    committed <- newTVarIO False
    let forkerEnv = ForkerEnv {
        foeLedgerSeq          = lseqVar
      , foeSwitchVar          = ldbSeq ldbEnv
      , foeSecurityParam      = ledgerDbCfgSecParam $ ldbCfg ldbEnv
      , foeTracer             = tr
      , foeResourcesToRemove  = toRemove
      , foeResourcesToRelease = toRelease
      , foeWasCommitted       = committed
      }
    atomically $ modifyTVar (ldbForkers ldbEnv) $ Map.insert forkerKey forkerEnv
    pure $ Forker {
        forkerClose                  = implForkerClose h forkerKey
      , forkerReadTables             = getForkerEnv1   h forkerKey implForkerReadTables
      , forkerRangeReadTables        = getForkerEnv1   h forkerKey implForkerRangeReadTables
      , forkerRangeReadTablesDefault = getForkerEnv1   h forkerKey implForkerRangeReadTablesDefault
      , forkerGetLedgerState         = getForkerEnvSTM h forkerKey implForkerGetLedgerState
      , forkerReadStatistics         = getForkerEnv    h forkerKey implForkerReadStatistics
      , forkerPush                   = getForkerEnv1   h forkerKey (implForkerPush ldbEnv rr)
      , forkerCommit                 = getForkerEnvSTM h forkerKey implForkerCommit
      }

-- | Will release all handles in the 'foeLedgerSeq'.
implForkerClose ::
     IOLike m
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
    whenJust menv closeForkerEnv

implForkerReadTables ::
     (MonadSTM m, GetTip l, MonadThrow m)
  => ForkerEnv m l blk
  -> LedgerTables l KeysMK
  -> m (LedgerTables l ValuesMK)
implForkerReadTables env ks = do
    traceWith (foeTracer env) ForkerReadTablesStart
    guardUncommitted env
      (\lseq -> do
        tbs <- read (tables $ forkerCurrentHandle lseq) ks
        traceWith (foeTracer env) ForkerReadTablesEnd
        pure tbs
      )

implForkerRangeReadTables ::
     -- (MonadSTM m, GetTip l, HasLedgerTables l)
  -- =>
  ForkerEnv m l blk
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
     -- (MonadSTM m, HasLedgerTables l, GetTip l)
  -- =>
  ForkerEnv m l blk
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
implForkerGetLedgerState env = forkerCurrent <$> readTVar (foeLedgerSeq env)

implForkerReadStatistics ::
     (MonadSTM m, GetTip l, MonadThrow m)
  => ForkerEnv m l blk
  -> m (Maybe Statistics)
implForkerReadStatistics env = do
  traceWith (foeTracer env) ForkerReadStatistics
  guardUncommitted env (fmap (fmap Statistics) . tablesSize . tables . forkerCurrentHandle)

implForkerPush ::
     (IOLike m, GetTip l, HasLedgerTables l)
  => LedgerDBEnv m l blk
  -> ResourceRegistry m
  -> ForkerEnv m l blk
  -> l DiffMK
  -> m ()
implForkerPush ldbEnv rr env newState = do
  traceWith (foeTracer env) ForkerPushStart
  guardUncommitted env $ \lseq -> do

    let (st, tbs) = (forgetLedgerTables newState, ltprj newState)

    -- allocate in the given outer registry
    (kOuter, newtbs) <- allocate rr (const $ duplicate (tables $ forkerCurrentHandle lseq)) close
    -- allocate in the ldb registry too
    (kInner, _) <- allocate (ldbRegistry ldbEnv) (const $ pure newtbs) close

    write newtbs tbs

    let lseq' = forkerPrune (foeSecurityParam env)
              $ forkerExtend (TempStateRef (StateRef st kInner newtbs) kOuter) lseq

    atomically $ do
      writeTVar (foeLedgerSeq env) lseq'
      -- Unless committed, when closing we should release these resources from the outer registry
      modifyTVar (foeResourcesToRelease env) (kOuter:)
      -- Unless committed, when closing we should remove these resources from the inner registry
      modifyTVar (foeResourcesToRemove env) (kInner:)
    traceWith (foeTracer env) ForkerPushEnd

implForkerCommit ::
     (MonadSTM m, GetTip l, MonadThrow (STM m))
  => ForkerEnv m l blk
  -> STM m ()
implForkerCommit env = do
  guardUncommittedSTM env $ \(ForkerLedgerSeq lseq) -> do
    let intersectionSlot = getTipSlot $ state $ AS.anchor lseq
    statesToClose <- stateTVar
      (foeSwitchVar env)
      (\(LedgerSeq olddb) -> fromMaybe theImpossible $ do
         (olddb', toClose) <- AS.splitAfterMeasure intersectionSlot (const True) olddb
         newdb <- AS.join (const $ const True) olddb' $ AS.mapPreservingMeasure tsrStateRef lseq
         pure (toClose, prune (foeSecurityParam env) (LedgerSeq newdb))
      )

    -- schedule to release (from the ldb registry) the resources that have been
    -- discarded
    writeTVar (foeResourcesToRelease env)
      . map resourceKey
      . AS.toOldestFirst
      $ statesToClose

    -- schedule to forget from the outer registry the resources that have been
    -- transferred
    writeTVar (foeResourcesToRemove env)
      . map tsrTempKey
      . AS.toOldestFirst
      $ lseq

    writeTVar (foeWasCommitted env) True

  where
    theImpossible = trace "BLAH" $
      error $ unwords [ "Critical invariant violation:"
                      , "Forker chain does no longer intersect with selected chain."
                      ]

{-------------------------------------------------------------------------------
  Acquiring consistent views
-------------------------------------------------------------------------------}

acquireAtTip ::
     (IOLike m, GetTip l)
  => LedgerDBEnv m l blk
  -> ResourceRegistry m
  -> m (StateRef m l)
acquireAtTip ldbEnv rr = do
  StateRef st _ tbs <- currentHandle <$> readTVarIO (ldbSeq ldbEnv)
  uncurry (StateRef st) <$> allocate rr (const $ duplicate tbs) close

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
  -> ResourceRegistry m
  -> m (Either GetForkerError (StateRef m l))
acquireAtPoint ldbEnv pt rr = do
      dblog <- readTVarIO (ldbSeq ldbEnv)
      let immTip = castPoint $ getTip $ anchor dblog
      case currentHandle <$> rollback pt dblog of
        Nothing     | pt < immTip -> pure $ Left PointTooOld
                    | otherwise   -> pure $ Left PointNotOnChain
        Just (StateRef st _ tbs) ->
              Right
            . uncurry (StateRef st)
          <$> allocate rr (const $ duplicate tbs) close

acquireAtFromTip ::
     forall m l blk. (
       IOLike m
     , IsLedger l
     )
  => LedgerDBEnv m l blk
  -> Word64
  -> ResourceRegistry m
  -> m (Either ExceededRollback (StateRef m l))
acquireAtFromTip ldbEnv n rr = do
      dblog <- readTVarIO (ldbSeq ldbEnv)
      case currentHandle <$> rollbackN n dblog of
        Nothing ->
          return $ Left $ ExceededRollback {
              rollbackMaximum   = maxRollback dblog
            , rollbackRequested = n
            }
        Just (StateRef st _ tbs) ->
              Right
            . uncurry (StateRef st)
          <$> allocate rr (const $ duplicate tbs) close

newForkerAtTip ::
     ( IOLike m
     , IsLedger l
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBHandle m l blk
  -> ResourceRegistry m
  -> String
  -> m (Forker m l blk)
newForkerAtTip h rr why = getEnv h $ \ldbEnv -> do
    acquireAtTip ldbEnv rr >>= newForker h ldbEnv why rr

newForkerAtPoint ::
     ( HeaderHash l ~ HeaderHash blk
     , IOLike m
     , IsLedger l
     , StandardHash l
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBHandle m l blk
  -> ResourceRegistry m
  -> String
  -> Point blk
  -> m (Either GetForkerError (Forker m l blk))
newForkerAtPoint h rr why pt = getEnv h $ \ldbEnv -> do
    acquireAtPoint ldbEnv pt rr >>= traverse (newForker h ldbEnv why rr)

newForkerAtFromTip ::
     ( IOLike m
     , IsLedger l
     , HasLedgerTables l
     , LedgerSupportsProtocol blk
     )
  => LedgerDBHandle m l blk
  -> ResourceRegistry m
  -> Word64
  -> String
  -> m (Either ExceededRollback (Forker m l blk))
newForkerAtFromTip h rr n why = getEnv h $ \ldbEnv -> do
    acquireAtFromTip ldbEnv n rr >>= traverse (newForker h ldbEnv why rr)

-- | Close all open block and header 'Follower's.
closeAllForkers ::
     IOLike m
  => LedgerDBEnv m l blk
  -> m ()
closeAllForkers ldbEnv = do
    toClose <- atomically $ stateTVar forkersVar (, Map.empty)
    mapM_ closeForkerEnv toClose
  where
    forkersVar = ldbForkers ldbEnv
