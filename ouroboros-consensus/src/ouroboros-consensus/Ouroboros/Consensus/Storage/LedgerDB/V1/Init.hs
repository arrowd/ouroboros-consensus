{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module Ouroboros.Consensus.Storage.LedgerDB.V1.Init (
    BackingStoreSelector (..)
  , mkInitDb
  ) where

import           Control.Monad
import           Control.Monad.Base
import           Control.Tracer (nullTracer)
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
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Validate as Validate
import           Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Common
import           Ouroboros.Consensus.Storage.LedgerDB.V1.DbChangelog
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.DbChangelog as DbCh
                     (empty, flushableLength)
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Flush
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Forker
import           Ouroboros.Consensus.Storage.LedgerDB.V1.Snapshots
import           Ouroboros.Consensus.Util
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry
import           Ouroboros.Network.AnchoredSeq (AnchoredSeq)
import qualified Ouroboros.Network.AnchoredSeq as AS

mkInitDb ::
  forall m blk.
  ( LedgerSupportsProtocol blk
  , IOLike m
  , LedgerDbSerialiseConstraints blk
  , MonadBase m m
  )
  => LedgerDBArgs Identity m blk
  -> BackingStoreSelector m
  -> ResolveBlock m blk
  -> InitDB m blk (DbChangelog' blk, BackingStore' m blk) (TestInternals m (ExtLedgerState blk) blk)
mkInitDb args bss getBlock =
  let bsTracer = nullTracer
  in InitDB {
    initFromGenesis = do
      st <- lgrGenesis
      let chlog = DbCh.empty (forgetLedgerTables st)
      backingStore <-
        newBackingStore bsTracer bss lgrHasFS (projectLedgerTables st)
      pure (chlog, backingStore)
  , initFromSnapshot = loadSnapshot bsTracer bss lgrTopLevelConfig lgrHasFS
  , closeDb = \(_, backingStore) -> bsClose backingStore
  , initApplyBlock = \cfg blk (chlog, bstore) -> do
      !chlog' <- onChangelogM (applyThenPush cfg blk (readKeySets bstore)) chlog
      -- It's OK to flush without a lock here, since the `LedgerDB` has not
      -- finishined initializing: only this thread has access to the backing
      -- store.
      chlog'' <- unsafeIgnoreWriteLock
        $ if defaultShouldFlush lgrFlushFrequency (flushableLength $ anchorlessChangelog chlog')
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
               , ldbDiskPolicy     = lgrDiskPolicy
               , ldbTracer         = lgrTracer
               , ldbCfg            = lgrTopLevelConfig
               , ldbHasFS          = lgrHasFS
               , ldbShouldFlush    = defaultShouldFlush lgrFlushFrequency
               , ldbQueryBatchSize = lgrQueryBatchSize
               , ldbResolveBlock   = getBlock
               , ldbSecParam       = configSecurityParam lgrTopLevelConfig
               , ldbBsTracer       = bsTracer
               }
      h <- LDBHandle <$> newTVarIO (LedgerDBOpen env)
      pure $ implMkLedgerDb h
  }
  where
    LedgerDBArgs {
        lgrHasFS
      , lgrTracer
      , lgrDiskPolicy
      , lgrTopLevelConfig
      , lgrGenesis
      , lgrFlushFrequency
      , lgrQueryBatchSize
      } = args

implMkLedgerDb ::
     forall m l blk.
     ( IOLike m
     , HasCallStack
     , IsLedger l
     , StandardHash l, HasLedgerTables l
     , HeaderHash l ~ HeaderHash blk
     , LedgerDbSerialiseConstraints blk
     , LedgerSupportsProtocol blk
     , MonadBase m m
     )
  => LedgerDBHandle m l blk
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
  => LedgerDBEnv m l blk
  -> STM m (l EmptyMK)
implGetVolatileTip = fmap (current . anchorlessChangelog) . readTVar . ldbChangelog

implGetImmutableTip ::
     MonadSTM m
  => LedgerDBEnv m l blk
  -> STM m (l EmptyMK)
implGetImmutableTip = fmap (anchor . anchorlessChangelog) . readTVar . ldbChangelog

implGetPastLedgerState ::
     ( MonadSTM m , HasHeader blk, IsLedger l, StandardHash l
     , HasLedgerTables l, HeaderHash l ~ HeaderHash blk )
  => LedgerDBEnv m l blk -> Point blk -> STM m (Maybe (l EmptyMK))
implGetPastLedgerState env point = getPastLedgerAt point . anchorlessChangelog <$> readTVar (ldbChangelog env)

implGetHeaderStateHistory ::
     (MonadSTM m, l ~ ExtLedgerState blk)
  => LedgerDBEnv m l blk -> STM m (HeaderStateHistory blk)
implGetHeaderStateHistory env = toHeaderStateHistory . adcStates . anchorlessChangelog <$> readTVar (ldbChangelog env)
  where
    toHeaderStateHistory ::
         AnchoredSeq (WithOrigin SlotNo) (ExtLedgerState blk EmptyMK) (ExtLedgerState blk EmptyMK)
      -> HeaderStateHistory blk
    toHeaderStateHistory =
          HeaderStateHistory
        . AS.bimap headerState headerState

implValidate ::
     forall m l blk. (
       IOLike m
     , LedgerSupportsProtocol blk
     , HasCallStack
     , l ~ ExtLedgerState blk
     , MonadBase m m
     )
  => LedgerDBHandle m l blk
  -> LedgerDBEnv m l blk
  -> ResourceRegistry m
  -> (TraceValidateEvent blk -> m ())
  -> BlockCache blk
  -> Word64
  -> [Header blk]
  -> m (ValidateResult m (ExtLedgerState blk) blk)
implValidate h ldbEnv =
  Validate.validate
    (ldbResolveBlock ldbEnv)
    (ldbCfg ldbEnv)
    (\l -> do
        prev <- readTVar (ldbPrevApplied ldbEnv)
        writeTVar (ldbPrevApplied ldbEnv) (foldl' (flip Set.insert) prev l))
    (readTVar (ldbPrevApplied ldbEnv))
    (newForkerAtFromTip h)


implGetPrevApplied :: MonadSTM m => LedgerDBEnv m l blk -> STM m (Set (RealPoint blk))
implGetPrevApplied env = readTVar (ldbPrevApplied env)

-- | Remove all points with a slot older than the given slot from the set of
-- previously applied points.
implGarbageCollect :: MonadSTM m => LedgerDBEnv m l blk -> SlotNo -> STM m ()
implGarbageCollect env slotNo = modifyTVar (ldbPrevApplied env) $
    Set.dropWhileAntitone ((< slotNo) . realPointSlot)

implTryTakeSnapshot ::
     ( l ~ ExtLedgerState blk
     , IOLike m, LedgerDbSerialiseConstraints blk, LedgerSupportsProtocol blk
     )
  => LedgerDBEnv m l blk -> Maybe (Time, Time) -> Word64 -> m SnapCounters
implTryTakeSnapshot env mTime nrBlocks =
    if onDiskShouldTakeSnapshot (ldbDiskPolicy env) (uncurry (flip diffTime) <$> mTime) nrBlocks then do
      void $ withReadLock (ldbLock env) (takeSnapshot
                                          (ldbChangelog env)
                                          (ldbCfg env)
                                          (LedgerDBSnapshotEvent >$< ldbTracer env)
                                          (ldbHasFS env)
                                          (ldbBackingStore env))
      void $ trimSnapshots
                (LedgerDBSnapshotEvent >$< ldbTracer env)
                (ldbHasFS env)
                (ldbDiskPolicy env)
      (`SnapCounters` 0) . Just <$> maybe getMonotonicTime (pure . snd) mTime
    else
      pure $ SnapCounters (fst <$> mTime) nrBlocks

-- If the DbChangelog in the LedgerDB can flush (based on the DiskPolicy
-- with which this LedgerDB was opened), flush differences to the backing
-- store. Note this acquires a write lock on the backing store.
implTryFlush ::
     (IOLike m, HasLedgerTables l, GetTip l)
  => LedgerDBEnv m l blk -> m ()
implTryFlush env = do
    ldb <- readTVarIO $ ldbChangelog env
    when (ldbShouldFlush env $ DbCh.flushableLength $ anchorlessChangelog ldb)
        (withWriteLock
          (ldbLock env)
          (flushLedgerDB (ldbChangelog env) (ldbBackingStore env))
        )

implCloseDB :: MonadSTM m => LedgerDBHandle m l blk -> m ()
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
