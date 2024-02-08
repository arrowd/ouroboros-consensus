{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE BangPatterns #-}

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
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Common
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Validate as Validate
import           Ouroboros.Consensus.Storage.LedgerDB.V2.Args as V2
import           Ouroboros.Consensus.Storage.LedgerDB.V2.Common
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.InMemory as InMemory
import           Ouroboros.Consensus.Storage.LedgerDB.V2.LedgerSeq
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.LSM as LSM
import           Ouroboros.Consensus.Util hiding (Dict (..))
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry
import           Ouroboros.Network.AnchoredSeq (AnchoredSeq)
import qualified Ouroboros.Network.AnchoredSeq as AS
import           System.FS.API

mkInitDb :: forall m blk.
            ( LedgerSupportsProtocol blk
            , IOLike m
            , MonadBase m m
            , LedgerDbSerialiseConstraints blk
            )
         => Complete LedgerDbArgs m blk
         -> Complete V2.LedgerDbFlavorArgs m
         -> Validate.ResolveBlock m blk
         -> InitDB (LedgerSeq' m blk) () m blk
mkInitDb args flavArgs getBlock =
  InitDB {
      initFromGenesis = emptyF =<< lgrGenesis
    , initFromSnapshot = \ds -> do
        traceMarkerIO "Loading snapshot"
        s <- loadSnapshot (configCodec . getExtLedgerCfg . ledgerDbCfg $ lgrConfig) lgrHasFS ds
        traceMarkerIO "Loaded snapshot"
        pure s
    , closeDb = closeLedgerSeq
    , initReapplyBlock = reapplyThenPush lgrRegistry
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
               , ldbSnapshotPolicy = defaultSnapshotPolicy (ledgerDbCfgSecParam lgrConfig) lgrSnapshotInterval
               , ldbTracer         = lgrTracer
               , ldbCfg            = lgrConfig
               , ldbHasFS          = lgrHasFS
               , ldbResolveBlock   = getBlock
               , ldbRegistry       = lgrRegistry
               }
        h <- LDBHandle <$> newTVarIO (LedgerDBOpen env)
        pure $ implMkLedgerDb h bss
    }
 where
   LedgerDbArgs {
       lgrConfig
     , lgrGenesis
     , lgrHasFS
     , lgrSnapshotInterval
     , lgrTracer
     , lgrRegistry
     } = args

   bss = case flavArgs of V2Args bss0 -> bss0

   emptyF :: ExtLedgerState blk ValuesMK
          -> m (LedgerSeq' m blk)
   emptyF st = do
     empty' st $ \tbs -> do
       !h <- case bss of
            InMemoryHandleArgs -> InMemory.newInMemoryLedgerTablesHandle lgrHasFS tbs
            LSMHandleArgs      -> LSM.newLSMLedgerTablesHandle tbs
       allocate
         lgrRegistry
         (const $ pure h )
         close

   loadSnapshot :: CodecConfig blk
                -> SomeHasFS m
                -> DiskSnapshot
                -> m (Either (SnapshotFailure blk) (LedgerSeq' m blk, RealPoint blk))
   loadSnapshot = case bss of
     InMemoryHandleArgs -> InMemory.loadSnapshot lgrRegistry
     LSMHandleArgs      -> LSM.loadSnapshot

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
  -> HandleArgs
  -> (LedgerDB m l blk, ())
implMkLedgerDb h bss = (LedgerDB {
      getVolatileTip         = getEnvSTM  h implGetVolatileTip
    , getImmutableTip        = getEnvSTM  h implGetImmutableTip
    , getPastLedgerState     = getEnvSTM1 h implGetPastLedgerState
    , getHeaderStateHistory  = getEnvSTM  h implGetHeaderStateHistory
    , getForkerAtTip         = newForkerAtTip h
    , getForkerAtPoint       = newForkerAtPoint h
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
implGetHeaderStateHistory env =
    toHeaderStateHistory . getLedgerSeq <$> readTVar (ldbSeq env)
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
    (\r w -> newForkerAtFromTip h r w "validate")

implGetPrevApplied :: MonadSTM m => LedgerDBEnv m l blk -> STM m (Set (RealPoint blk))
implGetPrevApplied env = readTVar (ldbPrevApplied env)

-- | Remove all points with a slot older than the given slot from the set of
-- previously applied points.
implGarbageCollect :: MonadSTM m => LedgerDBEnv m l blk -> SlotNo -> STM m ()
implGarbageCollect env slotNo = modifyTVar (ldbPrevApplied env) $
    Set.dropWhileAntitone ((< slotNo) . realPointSlot)

implTryTakeSnapshot ::
     forall m l blk.
     ( l ~ ExtLedgerState blk
     , IOLike m
     , LedgerSupportsProtocol blk
     , LedgerDbSerialiseConstraints blk
     )
  => HandleArgs
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
       InMemoryHandleArgs -> InMemory.takeSnapshot
       LSMHandleArgs      -> LSM.takeSnapshot

-- In the first version of the LedgerDB for UTxO-HD, there is a need to
-- periodically flush the accumulated differences to the disk. However, in the
-- second version there is no need to do so, and because of that, this function
-- does nothing in this case.
implTryFlush :: Applicative m => LedgerDBEnv m l blk -> m ()
implTryFlush _ = pure ()

implCloseDB :: IOLike m => LedgerDBHandle m l blk -> m ()
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
