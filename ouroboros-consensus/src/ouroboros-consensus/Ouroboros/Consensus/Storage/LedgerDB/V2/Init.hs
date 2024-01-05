{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module Ouroboros.Consensus.Storage.LedgerDB.V2.Init (
    V2BackendSelector (..)
  , mkInitDb
  ) where

import           Control.Monad (void)
import           Control.Monad.Base
import           Data.Foldable
import           Data.Functor.Contravariant ((>$<))
import           Data.Functor.Identity
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
import           Ouroboros.Consensus.Storage.ChainDB.Impl.BlockCache
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.Init
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.V2.Common
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.InMemory as InMemory
import           Ouroboros.Consensus.Storage.LedgerDB.V2.LedgerSeq
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.LSM as LSM
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Validate as Validate
import           Ouroboros.Consensus.Util
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry
import           Ouroboros.Network.AnchoredSeq (AnchoredSeq)
import qualified Ouroboros.Network.AnchoredSeq as AS

data V2BackendSelector = InMemory | LSM

mkInitDb :: forall m blk.
            ( LedgerSupportsProtocol blk
            , IOLike m
            , MonadBase m m
            , LedgerDbSerialiseConstraints blk
            )
         => LedgerDBArgs Identity m blk
         -> V2BackendSelector
         -> ResolveBlock m blk
         -> InitDB m blk (LedgerSeq' m blk) ()
mkInitDb args bss getBlock =
  InitDB {
      initFromGenesis = emptyF =<< lgrGenesis
    , initFromSnapshot = loadSnapshot lgrTopLevelConfig lgrHasFS
    , closeDb = closeLedgerSeq
    , initApplyBlock = applyThenPush
    , currentTip = ledgerState . current
    , mkLedgerDb = \lseq -> do
        let dbPrunedToImmDBTip = pruneToImmTipOnly lseq
        (varDB, prevApplied) <-
          (,) <$> newTVarIO dbPrunedToImmDBTip <*> newTVarIO Set.empty
        forkers <- newTVarIO Map.empty
        nextForkerKey <- newTVarIO (ForkerKey 0)
        let env = LedgerDBEnv {
                 ldbSeq            = varDB
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
               }
        h <- LDBHandle <$> newTVarIO (LedgerDBOpen env)
        pure $ implMkLedgerDb h bss
    }
 where
   LedgerDBArgs {..} = args
   emptyF st = empty' st $ case bss of
     InMemory -> InMemory.newInMemoryLedgerTablesHandle
     LSM      -> LSM.newLSMLedgerTablesHandle
   loadSnapshot = case bss of
     InMemory -> InMemory.loadSnapshot
     LSM      -> LSM.loadSnapshot

implMkLedgerDb ::
     forall m l blk.
     ( IOLike m
     , HasCallStack
     , IsLedger l
     , StandardHash l, HasLedgerTables l
     , HeaderHash l ~ HeaderHash blk
     , LedgerSupportsProtocol blk
     , LedgerDbSerialiseConstraints blk
     , MonadBase m m
     )
  => LedgerDBHandle m l blk
  -> V2BackendSelector
  -> (LedgerDB m l blk, ())
implMkLedgerDb h bss = (LedgerDB {
      getVolatileTip         = getEnvSTM  h implGetVolatileTip
    , getImmutableTip        = getEnvSTM  h implGetImmutableTip
    , getPastLedgerState     = getEnvSTM1 h implGetPastLedgerState
    , getHeaderStateHistory  = getEnvSTM  h implGetHeaderStateHistory
    , getForkerAtTip         = const $ newForkerAtTip h
    , getForkerAtPoint       = const $ newForkerAtPoint h
    , getForkerAtFromTip     = const $ newForkerAtFromTip h
    , validate               = getEnv5    h (implValidate h)
    , getPrevApplied         = getEnvSTM  h implGetPrevApplied
    , garbageCollect         = getEnvSTM1 h implGarbageCollect
    , tryTakeSnapshot        = getEnv2    h (implTryTakeSnapshot bss)
    , tryFlush               = getEnv     h implTryFlush
    , closeDB                = implCloseDB h
    }, ())

implGetVolatileTip ::
     (MonadSTM m, GetTip l)
  => LedgerDBEnv m l blk
  -> STM m (l EmptyMK)
implGetVolatileTip = fmap current . readTVar . ldbSeq

implGetImmutableTip ::
     MonadSTM m
  => LedgerDBEnv m l blk
  -> STM m (l EmptyMK)
implGetImmutableTip = fmap anchor . readTVar . ldbSeq

implGetPastLedgerState ::
     ( MonadSTM m , HasHeader blk, IsLedger l, StandardHash l
     , HeaderHash l ~ HeaderHash blk )
  => LedgerDBEnv m l blk -> Point blk -> STM m (Maybe (l EmptyMK))
implGetPastLedgerState env point = getPastLedgerAt point <$> readTVar (ldbSeq env)

implGetHeaderStateHistory ::
     (MonadSTM m, l ~ ExtLedgerState blk)
  => LedgerDBEnv m l blk -> STM m (HeaderStateHistory blk)
implGetHeaderStateHistory env = toHeaderStateHistory . getLedgerSeq <$> readTVar (ldbSeq env)
  where
    toHeaderStateHistory ::
         AnchoredSeq (WithOrigin SlotNo) (StateRef m (ExtLedgerState blk)) (StateRef m (ExtLedgerState blk))
      -> HeaderStateHistory blk
    toHeaderStateHistory =
          HeaderStateHistory
        . AS.bimap (headerState . state) (headerState . state)

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
    (const $ newForkerAtFromTip h)

implGetPrevApplied :: MonadSTM m => LedgerDBEnv m l blk -> STM m (Set (RealPoint blk))
implGetPrevApplied env = readTVar (ldbPrevApplied env)

-- | Remove all points with a slot older than the given slot from the set of
-- previously applied points.
implGarbageCollect :: MonadSTM m => LedgerDBEnv m l blk -> SlotNo -> STM m ()
implGarbageCollect env slotNo = modifyTVar (ldbPrevApplied env) $
    Set.dropWhileAntitone ((< slotNo) . realPointSlot)

implTryTakeSnapshot ::
     ( l ~ ExtLedgerState blk
     , IOLike m
     , LedgerSupportsProtocol blk
     , LedgerDbSerialiseConstraints blk
     )
  => V2BackendSelector
  -> LedgerDBEnv m l blk
  -> Maybe (Time, Time)
  -> Word64
  -> m SnapCounters
implTryTakeSnapshot bss env mTime nrBlocks =
    if onDiskShouldTakeSnapshot (ldbDiskPolicy env) (uncurry (flip diffTime) <$> mTime) nrBlocks then do
      void . takeSnapshot
                (ldbCfg env)
                (LedgerDBSnapshotEvent >$< ldbTracer env)
                (ldbHasFS env)
                . currentHandle
                =<< readTVarIO (ldbSeq env)
      void $ trimSnapshots
                (LedgerDBSnapshotEvent >$< ldbTracer env)
                (ldbHasFS env)
                (ldbDiskPolicy env)
      (`SnapCounters` 0) . Just <$> maybe getMonotonicTime (pure . snd) mTime
    else
      pure $ SnapCounters (fst <$> mTime) nrBlocks
  where
     takeSnapshot = case bss of
       InMemory -> InMemory.takeSnapshot
       LSM      -> InMemory.takeSnapshot

-- If the DbChangelog in the LedgerDB can flush (based on the DiskPolicy
-- with which this LedgerDB was opened), flush differences to the backing
-- store. Note this acquires a write lock on the backing store.
implTryFlush :: Applicative m => LedgerDBEnv m l blk -> m ()
implTryFlush _ = pure ()

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
