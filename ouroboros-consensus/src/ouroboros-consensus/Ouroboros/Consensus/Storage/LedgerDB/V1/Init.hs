{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MonoLocalBinds      #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module Ouroboros.Consensus.Storage.LedgerDB.V1.Init (mkInitDb) where

import           Control.Monad
import           Control.Monad.Base
import           Data.Foldable
import           Data.Functor.Contravariant ((>$<))
import qualified Data.Map.Strict as Map
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.HeaderStateHistory
                     (HeaderStateHistory (..))
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Ledger.Tables.Utils
import           Ouroboros.Consensus.Storage.ChainDB.Impl.BlockCache
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Validate as Validate
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Args
import           Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Common
import           Ouroboros.Consensus.Storage.LedgerDB.V1.DbChangelog
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.DbChangelog as DbCh
                     (empty, flushableLength)
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Flush
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Forker
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Lock
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Snapshots
import           Ouroboros.Consensus.Util hiding (Dict)
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry
import           Ouroboros.Consensus.Util.Singletons
import           Ouroboros.Network.AnchoredSeq (AnchoredSeq)
import qualified Ouroboros.Network.AnchoredSeq as AS

mkInitDb ::
  forall m blk impl.
  ( LedgerSupportsProtocol blk
  , IOLike m
  , LedgerDbSerialiseConstraints blk
  , MonadBase m m
  , SingI impl
  )
  => Complete LedgerDbArgs FlavorV1 impl m blk
  -> ResolveBlock m blk
  -> InitDB m blk (DbChangelog' blk, BackingStore' m blk) (TestInternals m (ExtLedgerState blk) blk)
mkInitDb args@(LedgerDbArgs { lgrFlavorArgs = flavorArgs }) getBlock =
  InitDB {
    initFromGenesis = do
      st <- lgrGenesis
      let chlog = DbCh.empty (forgetLedgerTables st)
      backingStore <-
        newBackingStore bsTracer (v1BackendArgs flavorArgs) lgrHasFS (projectLedgerTables st)
      pure (chlog, backingStore)
  , initFromSnapshot = loadSnapshot bsTracer (v1BackendArgs flavorArgs) (configCodec . getExtLedgerCfg . ledgerDbCfg $ lgrConfig) lgrHasFS
  , closeDb = \(_, backingStore) -> bsClose backingStore
  , initApplyBlock = \cfg blk (chlog, bstore) -> do
      !chlog' <- onChangelogM (applyThenPush cfg blk (readKeySets bstore)) chlog
      -- It's OK to flush without a lock here, since the `LedgerDB` has not
      -- finishined initializing: only this thread has access to the backing
      -- store.
      chlog'' <- unsafeIgnoreWriteLock
        $ if defaultShouldFlush flushFreq (flushableLength $ anchorlessChangelog chlog')
          then do
            let (toFlush, toKeep) = splitForFlushing chlog'
            mapM_ (flushIntoBackingStore bstore) toFlush
            pure toKeep
          else pure chlog'
      pure (chlog'', bstore)
  , currentTip = ledgerState . current . anchorlessChangelog . fst
  , mkLedgerDb = \(db, lgrBackingStore) -> do
      let dbPrunedToImmDBTip = onChangelog pruneToImmTipOnly db
      (varDB, prevApplied) <-
        (,) <$> newTVarIO dbPrunedToImmDBTip <*> newTVarIO Set.empty
      flushLock <- mkLedgerDBLock
      forkers <- newTVarIO Map.empty
      nextForkerKey <- newTVarIO (ForkerKey 0)
      let env = LedgerDBEnv {
                 ldbChangelog      = varDB
               , ldbBackingStore   = lgrBackingStore
               , ldbLock           = flushLock
               , ldbPrevApplied    = prevApplied
               , ldbForkers        = forkers
               , ldbNextForkerKey  = nextForkerKey
               , ldbSnapshotPolicy = lgrSnapshotPolicy
               , ldbTracer         = lgrTracer
               , ldbCfg            = lgrConfig
               , ldbHasFS          = lgrHasFS
               , ldbShouldFlush    = defaultShouldFlush flushFreq
               , ldbQueryBatchSize = lgrQueryBatchSize
               , ldbResolveBlock   = getBlock
               , ldbSecParam       = ledgerDbCfgSecParam lgrConfig
               , ldbFlavorSpecificTracer = bsTracer
               }
      h <- LDBHandle <$> newTVarIO (LedgerDBOpen env)
      pure $ implMkLedgerDb h
  }
  where
    bsTracer = LedgerDBFlavorImplEvent >$< lgrTracer

    LedgerDbArgs {
        lgrHasFS
      , lgrTracer
      , lgrSnapshotPolicy
      , lgrConfig
      , lgrGenesis
      , lgrQueryBatchSize
      } = args

    V1Args flushFreq _ = flavorArgs

implMkLedgerDb ::
     forall impl m l blk.
     ( IOLike m
     , HasCallStack
     , IsLedger l
     , StandardHash l, HasLedgerTables l
     , HeaderHash l ~ HeaderHash blk
     , LedgerDbSerialiseConstraints blk
     , LedgerSupportsProtocol blk
     , MonadBase m m
     )
  => LedgerDBHandle impl m l blk
  -> (LedgerDB m l blk, TestInternals m l blk)
implMkLedgerDb h = (LedgerDB {
      getVolatileTip         = getEnvSTM  h implGetVolatileTip
    , getImmutableTip        = getEnvSTM  h implGetImmutableTip
    , getPastLedgerState     = getEnvSTM1 h implGetPastLedgerState
    , getHeaderStateHistory  = getEnvSTM  h implGetHeaderStateHistory
    , getForkerAtTip         = newForkerAtTip h
    , getForkerAtPoint       = newForkerAtPoint h
    , getForkerAtFromTip     = newForkerAtFromTip h
    , validate               = getEnv5    h (implValidate h)
    , getPrevApplied         = getEnvSTM  h implGetPrevApplied
    , garbageCollect         = getEnvSTM1 h implGarbageCollect
    , tryTakeSnapshot        = getEnv2    h implTryTakeSnapshot
    , tryFlush               = getEnv     h implTryFlush
    , closeDB                = implCloseDB h
    }, TestInternals)

implGetVolatileTip ::
     (MonadSTM m, GetTip l)
  => LedgerDBEnv impl m l blk
  -> STM m (l EmptyMK)
implGetVolatileTip = fmap (current . anchorlessChangelog) . readTVar . ldbChangelog

implGetImmutableTip ::
     MonadSTM m
  => LedgerDBEnv impl m l blk
  -> STM m (l EmptyMK)
implGetImmutableTip = fmap (anchor . anchorlessChangelog) . readTVar . ldbChangelog

implGetPastLedgerState ::
     ( MonadSTM m , HasHeader blk, IsLedger l, StandardHash l
     , HasLedgerTables l, HeaderHash l ~ HeaderHash blk )
  => LedgerDBEnv impl m l blk -> Point blk -> STM m (Maybe (l EmptyMK))
implGetPastLedgerState env point = getPastLedgerAt point . anchorlessChangelog <$> readTVar (ldbChangelog env)

implGetHeaderStateHistory ::
     (MonadSTM m, l ~ ExtLedgerState blk)
  => LedgerDBEnv impl m l blk -> STM m (HeaderStateHistory blk)
implGetHeaderStateHistory env = toHeaderStateHistory . adcStates . anchorlessChangelog <$> readTVar (ldbChangelog env)
  where
    toHeaderStateHistory ::
         AnchoredSeq (WithOrigin SlotNo) (ExtLedgerState blk EmptyMK) (ExtLedgerState blk EmptyMK)
      -> HeaderStateHistory blk
    toHeaderStateHistory =
          HeaderStateHistory
        . AS.bimap headerState headerState

implValidate ::
     forall impl m l blk. (
       IOLike m
     , LedgerSupportsProtocol blk
     , HasCallStack
     , l ~ ExtLedgerState blk
     , MonadBase m m
     )
  => LedgerDBHandle impl m l blk
  -> LedgerDBEnv impl m l blk
  -> ResourceRegistry m
  -> (TraceValidateEvent blk -> m ())
  -> BlockCache blk
  -> Word64
  -> [Header blk]
  -> m (ValidateResult m (ExtLedgerState blk) blk)
implValidate h ldbEnv =
  Validate.validate
    (ldbResolveBlock ldbEnv)
    (getExtLedgerCfg . ledgerDbCfg $ ldbCfg ldbEnv)
    (\l -> do
        prev <- readTVar (ldbPrevApplied ldbEnv)
        writeTVar (ldbPrevApplied ldbEnv) (foldl' (flip Set.insert) prev l))
    (readTVar (ldbPrevApplied ldbEnv))
    (newForkerAtFromTip h)


implGetPrevApplied :: MonadSTM m => LedgerDBEnv impl m l blk -> STM m (Set (RealPoint blk))
implGetPrevApplied env = readTVar (ldbPrevApplied env)

-- | Remove all points with a slot older than the given slot from the set of
-- previously applied points.
implGarbageCollect :: MonadSTM m => LedgerDBEnv impl m l blk -> SlotNo -> STM m ()
implGarbageCollect env slotNo = modifyTVar (ldbPrevApplied env) $
    Set.dropWhileAntitone ((< slotNo) . realPointSlot)

implTryTakeSnapshot ::
     ( l ~ ExtLedgerState blk
     , IOLike m, LedgerDbSerialiseConstraints blk, LedgerSupportsProtocol blk
     )
  => LedgerDBEnv impl m l blk -> Maybe (Time, Time) -> Word64 -> m SnapCounters
implTryTakeSnapshot env mTime nrBlocks =
    if onDiskShouldTakeSnapshot (ldbSnapshotPolicy env) (uncurry (flip diffTime) <$> mTime) nrBlocks then do
      void $ withReadLock (ldbLock env) (takeSnapshot
                                          (ldbChangelog env)
                                          (configCodec . getExtLedgerCfg . ledgerDbCfg $ ldbCfg env)
                                          (LedgerDBSnapshotEvent >$< ldbTracer env)
                                          (ldbHasFS env)
                                          (ldbBackingStore env))
      void $ trimSnapshots
                (LedgerDBSnapshotEvent >$< ldbTracer env)
                (ldbHasFS env)
                (ldbSnapshotPolicy env)
      (`SnapCounters` 0) . Just <$> maybe getMonotonicTime (pure . snd) mTime
    else
      pure $ SnapCounters (fst <$> mTime) nrBlocks

-- If the DbChangelog in the LedgerDB can flush (based on the SnapshotPolicy
-- with which this LedgerDB was opened), flush differences to the backing
-- store. Note this acquires a write lock on the backing store.
implTryFlush ::
     (IOLike m, HasLedgerTables l, GetTip l)
  => LedgerDBEnv impl m l blk -> m ()
implTryFlush env = do
    ldb <- readTVarIO $ ldbChangelog env
    when (ldbShouldFlush env $ DbCh.flushableLength $ anchorlessChangelog ldb)
        (withWriteLock
          (ldbLock env)
          (flushLedgerDB (ldbChangelog env) (ldbBackingStore env))
        )

implCloseDB :: MonadSTM m => LedgerDBHandle impl m l blk -> m ()
implCloseDB (LDBHandle varState) = do
    mbOpenEnv <- atomically $ readTVar varState >>= \case
      -- Idempotent
      LedgerDBClosed   -> return Nothing
      LedgerDBOpen env -> do
        writeTVar varState LedgerDBClosed
        return $ Just env

    -- Only when the LedgerDB was open
    whenJust mbOpenEnv $ \env -> do
      closeAllForkers env
      bsClose (ldbBackingStore env)
