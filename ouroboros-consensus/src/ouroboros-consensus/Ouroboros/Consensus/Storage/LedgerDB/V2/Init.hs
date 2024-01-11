{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MonoLocalBinds      #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module Ouroboros.Consensus.Storage.LedgerDB.V2.Init (mkInitDb) where

import           Control.Monad (void)
import           Control.Monad.Base
import           Control.Tracer
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
import           Ouroboros.Consensus.Storage.ChainDB.Impl.BlockCache
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Validate as Validate
import           Ouroboros.Consensus.Storage.LedgerDB.V2.Common
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.InMemory as InMemory
import           Ouroboros.Consensus.Storage.LedgerDB.V2.LedgerSeq
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.LSM as LSM
import           Ouroboros.Consensus.Util
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry
import           Ouroboros.Consensus.Util.Singletons
import           Ouroboros.Network.AnchoredSeq (AnchoredSeq)
import qualified Ouroboros.Network.AnchoredSeq as AS
import           System.FS.API

mkInitDb :: forall m blk impl.
            ( LedgerSupportsProtocol blk
            , IOLike m
            , MonadBase m m
            , LedgerDbSerialiseConstraints blk
            )
         => Complete LedgerDbArgs FlavorV2 impl m blk
         -> ResolveBlock m blk
         -> InitDB m blk (LedgerSeq' m blk) ()
mkInitDb args@(LedgerDbArgs { lgrFlavorArgs = _ }) getBlock =
  InitDB {
      initFromGenesis = emptyF =<< lgrGenesis
    , initFromSnapshot = loadSnapshot (configCodec . getExtLedgerCfg . ledgerDbCfg $ lgrConfig) lgrHasFS
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
               , ldbSnapshotPolicy = lgrSnapshotPolicy
               , ldbTracer         = lgrTracer
               , ldbCfg            = lgrConfig
               , ldbHasFS          = lgrHasFS
               , ldbQueryBatchSize = lgrQueryBatchSize
               , ldbResolveBlock   = getBlock
               , ldbSecParam       = ledgerDbCfgSecParam lgrConfig
               }
        h <- LDBHandle <$> newTVarIO (LedgerDBOpen env)
        pure $ implMkLedgerDb h bss
    }
 where
   LedgerDbArgs {
       lgrConfig
     , lgrGenesis
     , lgrHasFS
     , lgrSnapshotPolicy
     , lgrTracer
     , lgrQueryBatchSize
     } = args

   bss = sing :: Sing impl

   emptyF :: ExtLedgerState blk ValuesMK
          -> m (LedgerSeq' m blk)
   emptyF st = empty' st $ case bss of
     SInMemory -> InMemory.newInMemoryLedgerTablesHandle
     SOnDisk   -> LSM.newLSMLedgerTablesHandle

   loadSnapshot :: CodecConfig blk
                -> SomeHasFS m
                -> DiskSnapshot
                -> m (Either (SnapshotFailure blk) (LedgerSeq' m blk, RealPoint blk))
   loadSnapshot = case bss of
     SInMemory -> InMemory.loadSnapshot
     SOnDisk   -> LSM.loadSnapshot

implMkLedgerDb ::
     forall m l blk (impl :: LedgerDbStorageFlavor).
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
  -> Sing impl
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
    (getExtLedgerCfg . ledgerDbCfg $ ldbCfg ldbEnv)
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
     forall m l blk (impl :: LedgerDbStorageFlavor).
     ( l ~ ExtLedgerState blk
     , IOLike m
     , LedgerSupportsProtocol blk
     , LedgerDbSerialiseConstraints blk
     )
  => Sing impl
  -> LedgerDBEnv m l blk
  -> Maybe (Time, Time)
  -> Word64
  -> m SnapCounters
implTryTakeSnapshot bss env mTime nrBlocks =
    if onDiskShouldTakeSnapshot (ldbSnapshotPolicy env) (uncurry (flip diffTime) <$> mTime) nrBlocks then do
      void . takeSnapshot
                (configCodec . getExtLedgerCfg . ledgerDbCfg $ ldbCfg env)
                (LedgerDBSnapshotEvent >$< ldbTracer env)
                (ldbHasFS env)
                . currentHandle
                =<< readTVarIO (ldbSeq env)
      void $ trimSnapshots
                (LedgerDBSnapshotEvent >$< ldbTracer env)
                (ldbHasFS env)
                (ldbSnapshotPolicy env)
      (`SnapCounters` 0) . Just <$> maybe getMonotonicTime (pure . snd) mTime
    else
      pure $ SnapCounters (fst <$> mTime) nrBlocks
  where
     takeSnapshot :: CodecConfig blk
                  -> Tracer m (TraceSnapshotEvent blk)
                  -> SomeHasFS m
                  -> StateRef m (ExtLedgerState blk)
                  -> m (Maybe (DiskSnapshot, RealPoint blk))
     takeSnapshot = case bss of
       SInMemory -> InMemory.takeSnapshot
       SOnDisk   -> InMemory.takeSnapshot

-- If the DbChangelog in the LedgerDB can flush (based on the SnapshotPolicy
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
