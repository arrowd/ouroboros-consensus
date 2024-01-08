{-# LANGUAGE BangPatterns           #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE NamedFieldPuns         #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}

-- | Logic for initializing the LedgerDB.
--
-- Each implementation of the LedgerDB has to provide an instantiation of
-- 'InitDB'. See 'initialize' for a description of the initialization process.
module Ouroboros.Consensus.Storage.LedgerDB.Impl.Init (
    -- * Arguments
    LedgerDBArgs (..)
  , LedgerDbImplementationSelector (..)
  , defaultArgs
    -- * Find blocks
  , ResolveBlock
  , ResolvesBlocks (..)
    -- * Should flush
  , FlushFrequency (..)
  , defaultShouldFlush
    -- * Initialization interface
  , InitDB (..)
    -- * Initialization logic
  , InitLog (..)
  , openDB
  , openDBInternal
  ) where

import           Control.Monad (when)
import           Control.Monad.Except (ExceptT, runExceptT)
import           Control.Monad.IO.Class
import           Control.Monad.Reader
import           Control.Tracer
import           Data.Functor.Contravariant ((>$<))
import           Data.Word
import           GHC.Generics hiding (from)
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.Inspect
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Storage.ImmutableDB.API (ImmutableDB)
import           Ouroboros.Consensus.Storage.ImmutableDB.Impl.Stream
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.Impl.LMDB as LMDB
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Network.Block
import           System.FS.API
import           System.FS.API.Types

{-------------------------------------------------------------------------------
  Finding blocks
-------------------------------------------------------------------------------}

-- | Resolve a block
--
-- Resolving a block reference to the actual block lives in @m@ because
-- it might need to read the block from disk (and can therefore not be
-- done inside an STM transaction).
--
-- NOTE: The ledger DB will only ask the 'ChainDB' for blocks it knows
-- must exist. If the 'ChainDB' is unable to fulfill the request, data
-- corruption must have happened and the 'ChainDB' should trigger
-- validation mode.
type ResolveBlock m blk = RealPoint blk -> m blk

-- | Monads in which we can resolve blocks
--
-- To guide type inference, we insist that we must be able to infer the type
-- of the block we are resolving from the type of the monad.
class Monad m => ResolvesBlocks m blk | m -> blk where
  doResolveBlock :: ResolveBlock m blk

instance Monad m => ResolvesBlocks (ReaderT (ResolveBlock m blk) m) blk where
  doResolveBlock r = ReaderT $ \f -> f r

-- Quite a specific instance so we can satisfy the fundep
instance Monad m
      => ResolvesBlocks (ExceptT e (ReaderT (ResolveBlock m blk) m)) blk where
  doResolveBlock = lift . doResolveBlock

{-------------------------------------------------------------------------------
  Arguments
-------------------------------------------------------------------------------}

-- | Arguments required to initialize a LedgerDB.
data LedgerDBArgs f m blk = LedgerDBArgs {
      lgrDiskPolicy      :: DiskPolicy
    , lgrGenesis         :: HKD f (m (ExtLedgerState blk ValuesMK))
    , lgrHasFS           :: SomeHasFS m
    , lgrTopLevelConfig  :: HKD f (TopLevelConfig blk)
    , lgrTracer          :: Tracer m (TraceLedgerDBEvent blk)
    , lgrFlushFrequency  :: FlushFrequency
    , lgrQueryBatchSize  :: QueryBatchSize
    , lgrBackendSelector :: LedgerDbImplementationSelector m
    }

data LedgerDbImplementationSelector m where
  V1InMemory ::                                 LedgerDbImplementationSelector m
  V1LMDB     :: MonadIO m => LMDB.LMDBLimits -> LedgerDbImplementationSelector m
  V2InMemory ::                                 LedgerDbImplementationSelector m
  V2LSM      ::                                 LedgerDbImplementationSelector m

-- | Default arguments
defaultArgs ::
     Applicative m
  => SomeHasFS m
  -> DiskPolicy
  -> FlushFrequency
  -> QueryBatchSize
  -> LedgerDbImplementationSelector m
  -> LedgerDBArgs Defaults m blk
defaultArgs lgrHasFS diskPolicy flushFreq qbatchSize lgrBackendSelector = LedgerDBArgs {
      lgrDiskPolicy           = diskPolicy
    , lgrGenesis              = NoDefault
    , lgrHasFS
    , lgrTopLevelConfig       = NoDefault
    , lgrTracer               = nullTracer
    , lgrFlushFrequency       = flushFreq
    , lgrQueryBatchSize       = qbatchSize
    , lgrBackendSelector
    }

-- | The number of diffs in the immutable part of the chain that we have to see
-- before we flush the ledger state to disk. See 'onDiskShouldFlush'.
--
-- INVARIANT: Should be at least 0.
data FlushFrequency =
  -- | A default value, which is determined by a specific 'DiskPolicy'. See
    -- 'defaultDiskPolicy' as an example.
    DefaultFlushFrequency
    -- | A requested value: the number of diffs in the immutable part of the
    -- chain required before flushing.
  | RequestedFlushFrequency Word64
  deriving (Show, Eq, Generic)

defaultShouldFlush :: FlushFrequency -> (Word64 -> Bool)
defaultShouldFlush requestedFlushFrequency = case requestedFlushFrequency of
      RequestedFlushFrequency value -> (>= value)
      DefaultFlushFrequency         -> (>= 100)

-- | Initialization log
--
-- The initialization log records which snapshots from disk were considered,
-- in which order, and why some snapshots were rejected. It is primarily useful
-- for monitoring purposes.
data InitLog blk =
    -- | Defaulted to initialization from genesis
    --
    -- NOTE: Unless the blockchain is near genesis, or this is the first time we
    -- boot the node, we should see this /only/ if data corruption occurred.
    InitFromGenesis

    -- | Used a snapshot corresponding to the specified tip
  | InitFromSnapshot DiskSnapshot (RealPoint blk)

    -- | Initialization skipped a snapshot
    --
    -- We record the reason why it was skipped.
    --
    -- NOTE: We should /only/ see this if data corruption occurred.
  | InitFailure DiskSnapshot (SnapshotFailure blk) (InitLog blk)
  deriving (Show, Eq, Generic)

-- | Functions required to initialize a LedgerDB
data InitDB m blk db internal = InitDB {
    initFromGenesis  :: !(m db)
  , initFromSnapshot :: !(DiskSnapshot -> m (Either (SnapshotFailure blk) (db, RealPoint blk)))
  , closeDb          :: !(db -> m ())
  , initApplyBlock   :: !(LedgerDbCfg (ExtLedgerState blk) -> blk -> db -> m db)
  , currentTip       :: !(db -> LedgerState blk EmptyMK)
  , mkLedgerDb       :: !(db -> m (LedgerDB m (ExtLedgerState blk) blk, internal))
  }

-- | Initialize the ledger DB from the most recent snapshot on disk
--
-- If no such snapshot can be found, use the genesis ledger DB. Returns the
-- initialized DB as well as a log of the initialization and the number of
-- blocks replayed between the snapshot and the tip of the immutable DB.
--
-- We do /not/ catch any exceptions thrown during streaming; should any be
-- thrown, it is the responsibility of the 'ChainDB' to catch these
-- and trigger (further) validation. We only discard snapshots if
--
-- * We cannot deserialise them, or
--
-- * they are /ahead/ of the chain, they refer to a slot which is later than the
--     last slot in the immutable db.
--
-- Note that after initialization, the ledger db should be pruned so that no
-- ledger states are considered volatile. Otherwise we would be able to rollback
-- the immutable DB.
--
-- We do /not/ attempt to use multiple ledger states from disk to construct the
-- ledger DB. Instead we load only a /single/ ledger state from disk, and
-- /compute/ all subsequent ones. This is important, because the ledger states
-- obtained in this way will (hopefully) share much of their memory footprint
-- with their predecessors.
initialize ::
     forall m blk db internal.
     ( IOLike m
     , LedgerSupportsProtocol blk
     , InspectLedger blk
     , HasCallStack
     )
  => Tracer m (TraceLedgerDBEvent blk)
  -> SomeHasFS m
  -> LedgerDbCfg (ExtLedgerState blk)
  -> StreamAPI m blk blk
  -> Point blk
  -> InitDB m blk db internal
  -> m (InitLog blk, db, Word64)
initialize tracer
           hasFS
           cfg
           stream
           replayGoal
           dbIface  =
    listSnapshots hasFS >>= tryNewestFirst id
  where
    InitDB {initFromGenesis, initFromSnapshot, closeDb} = dbIface

    tryNewestFirst :: (InitLog blk -> InitLog blk)
                   -> [DiskSnapshot]
                   -> m ( InitLog   blk
                        , db
                        , Word64
                        )
    tryNewestFirst acc [] = do
      -- We're out of snapshots. Start at genesis
      traceWith (LedgerReplayStartEvent >$< tracer) ReplayFromGenesis
      let replayTracer' = decorateReplayTracerWithStart (Point Origin) replayTracer
      initDb <- initFromGenesis
      eDB <- runExceptT $ replayStartingWith
                            replayTracer'
                            cfg
                            stream
                            initDb
                            (Point Origin)
                            dbIface

      case eDB of
        Left err -> do
          closeDb initDb
          error $ "Invariant violation: invalid immutable chain " <> show err
        Right (db, replayed) -> do
          return ( acc InitFromGenesis
                 , db
                 , replayed
                 )

    tryNewestFirst acc (s:ss) = do
      eInitDb <- initFromSnapshot s
      case eInitDb of
        Left err -> do
          when (diskSnapshotIsTemporary s || err == InitFailureGenesis) $
            deleteSnapshot hasFS s
          traceWith (LedgerDBSnapshotEvent >$< tracer) . InvalidSnapshot s $ err
          tryNewestFirst (acc . InitFailure s err) ss
        Right (initDb, pt) -> do
          let pt' = realPointToPoint pt
          traceWith (LedgerReplayStartEvent >$< tracer) (ReplayFromSnapshot s (ReplayStart pt'))
          let replayTracer' = decorateReplayTracerWithStart pt' replayTracer
          eDB <- runExceptT
                   $ replayStartingWith
                       replayTracer'
                       cfg
                       stream
                       initDb
                       pt'
                       dbIface
          case eDB of
            Left err -> do
              traceWith (LedgerDBSnapshotEvent >$< tracer) . InvalidSnapshot s $ err
              when (diskSnapshotIsTemporary s) $ deleteSnapshot hasFS s
              closeDb initDb
              tryNewestFirst (acc . InitFailure s err) ss
            Right (db, replayed) -> do
              return (acc (InitFromSnapshot s pt), db, replayed)

    replayTracer = decorateReplayTracerWithGoal
                                       replayGoal
                                       (LedgerReplayProgressEvent  >$< tracer)

-- | Replay all blocks in the Immutable database using the 'StreamAPI' provided
-- on top of the given @LedgerDB' blk@.
--
-- It will also return the number of blocks that were replayed.
replayStartingWith ::
     forall m blk db internal. (
         IOLike m
       , LedgerSupportsProtocol blk
       , InspectLedger blk
       , HasCallStack
       )
  => Tracer m (ReplayStart blk -> ReplayGoal blk -> TraceReplayProgressEvent blk)
  -> LedgerDbCfg (ExtLedgerState blk)
  -> StreamAPI m blk blk
  -> db
  -> Point blk
  -> InitDB m blk db internal
  -> ExceptT (SnapshotFailure blk) m (db, Word64)
replayStartingWith tracer cfg stream initDb from InitDB{initApplyBlock, currentTip} = do
    streamAll stream from
        InitFailureTooRecent
        (initDb, 0)
        push
  where
    push :: blk
         -> (db, Word64)
         -> m (db, Word64)
    push blk (!db, !replayed) = do
        !db' <- initApplyBlock cfg blk db

        let replayed' :: Word64
            !replayed' = replayed + 1

            events :: [LedgerEvent blk]
            events = inspectLedger
                       (getExtLedgerCfg (ledgerDbCfg cfg))
                       (currentTip db)
                       (currentTip db')

        traceWith tracer (ReplayedBlock (blockRealPoint blk) events)
        return (db', replayed')

{-------------------------------------------------------------------------------
  Opening a LedgerDB
-------------------------------------------------------------------------------}

-- | Open the ledger DB
--
-- In addition to the ledger DB also returns the number of immutable blocks that
-- were replayed.
openDB ::
  forall m l blk db internal.
  ( IOLike m
  , LedgerSupportsProtocol blk
  , InspectLedger blk
  , HasCallStack
  , l ~ ExtLedgerState blk
  )
  => LedgerDBArgs Identity m blk
  -- ^ Stateless initializaton arguments
  -> InitDB m blk db internal
  -- ^ How to initialize the db.
  -> ImmutableDB m blk
  -- ^ Reference to the immutable DB
  --
  -- After reading a snapshot from disk, the ledger DB will be brought up to
  -- date with tip of the immutable DB. The corresponding ledger state at the
  -- tip can then be used as the starting point for chain selection in the
  -- ChainDB driver.
  -> Point blk
  -> m (LedgerDB m l blk, Word64)
openDB args initDb immutableDB replayGoal =
    f <$> openDBInternal args initDb immutableDB replayGoal
  where f (ldb, replayCounter, _) = (ldb, replayCounter)

-- | Open the ledger DB and expose internals for testing purposes
openDBInternal ::
  forall m l blk db internal.
  ( IOLike m
  , LedgerSupportsProtocol blk
  , InspectLedger blk
  , HasCallStack
  , l ~ ExtLedgerState blk
  )
  => LedgerDBArgs Identity m blk
  -> InitDB m blk db internal
  -> ImmutableDB m blk
  -> Point blk
  -> m (LedgerDB m l blk, Word64, internal)
openDBInternal args@LedgerDBArgs { lgrHasFS = SomeHasFS fs } initDb immutableDB replayGoal = do
    createDirectoryIfMissing fs True (mkFsPath [])
    (_initLog, db, replayCounter) <-
          initialize
            lgrTracer
            lgrHasFS
            (configLedgerDb lgrTopLevelConfig)
            (streamAPI immutableDB)
            replayGoal
            initDb
    (ledgerDb, internal) <- mkLedgerDb initDb db
    return (ledgerDb, replayCounter, internal)

  where
    LedgerDBArgs {
        lgrHasFS
      , lgrTopLevelConfig
      , lgrTracer
      } = args
