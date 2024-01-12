{-# LANGUAGE ConstraintKinds          #-}
{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE DeriveGeneric            #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE NamedFieldPuns           #-}
{-# LANGUAGE PolyKinds                #-}
{-# LANGUAGE RankNTypes               #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}
{-# LANGUAGE UndecidableInstances     #-}
-- |

module Ouroboros.Consensus.Storage.LedgerDB.Impl.Args (
    LedgerDbArgs (..)
  , SomeLedgerDbArgs (..)
  , defaultArgs
    -- * Tracing
  , FlavorImplSpecificTrace
  , ReplayGoal (..)
  , ReplayStart (..)
  , TraceLedgerDBEvent (..)
  , TraceReplayEvent (..)
  , TraceReplayProgressEvent (..)
  , TraceReplayStartEvent (..)
  , decorateReplayTracerWithGoal
  , decorateReplayTracerWithStart
  ) where

import           Control.Tracer
import           Data.Functor.Contravariant ((>$<))
import           Data.Kind
import           GHC.Generics
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.Inspect
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.Singletons
import           System.FS.API

{-------------------------------------------------------------------------------
  Arguments
-------------------------------------------------------------------------------}

-- | Arguments required to initialize a LedgerDB.
type LedgerDbArgs :: (Type -> Type) -> LedgerDbFlavor -> LedgerDbStorageFlavor -> (Type -> Type) -> Type -> Type
data LedgerDbArgs f flavor impl m blk = (SingI flavor, SingI impl) => LedgerDbArgs {
      lgrSnapshotPolicy :: HKD f SnapshotPolicy
    , lgrGenesis        :: HKD f (m (ExtLedgerState blk ValuesMK))
    , lgrHasFS          :: SomeHasFS m
    , lgrConfig         :: HKD f (LedgerDbCfg (ExtLedgerState blk))
    , lgrTracer         :: Tracer m (TraceLedgerDBEvent flavor impl blk)
    , lgrQueryBatchSize :: QueryBatchSize
    , lgrFlavorArgs     :: LedgerDbFlavorArgs flavor impl m
    }

data SomeLedgerDbArgs f m blk where
  SomeLedgerDbArgs ::
       LedgerDbArgs f flavor impl m blk
    -> SomeLedgerDbArgs f m blk

-- | Default arguments
defaultArgs ::
     ( HasFlavorArgs flavor impl m
     , Applicative m
     )
  => SomeHasFS m
  -> Incomplete LedgerDbArgs flavor impl m blk
defaultArgs lgrHasFS = LedgerDbArgs {
      lgrSnapshotPolicy = NoDefault
    , lgrGenesis        = NoDefault
    , lgrHasFS
    , lgrConfig         = NoDefault
    , lgrTracer         = nullTracer
    , lgrQueryBatchSize = DefaultQueryBatchSize
    , lgrFlavorArgs     = defaultFlavorArgs
    }

{-------------------------------------------------------------------------------
  Tracing
-------------------------------------------------------------------------------}

data family FlavorImplSpecificTrace (flavor :: k) (impl :: l)

data TraceLedgerDBEvent flavor impl blk =
      LedgerDBSnapshotEvent   !(TraceSnapshotEvent blk)
    | LedgerReplayEvent       !(TraceReplayEvent blk)
    | LedgerDBForkerEvent     !TraceForkerEventWithKey
    | LedgerDBFlavorImplEvent !(FlavorImplSpecificTrace flavor impl)
  deriving (Generic)

deriving instance
  (StandardHash blk, Show (FlavorImplSpecificTrace flavor impl), InspectLedger blk)
  => Show (TraceLedgerDBEvent flavor impl blk)
deriving instance
  (StandardHash blk, Eq (FlavorImplSpecificTrace flavor impl), InspectLedger blk)
  => Eq (TraceLedgerDBEvent flavor impl blk)

{-------------------------------------------------------------------------------
  Trace replay events
-------------------------------------------------------------------------------}

data TraceReplayEvent blk =
      TraceReplayStartEvent (TraceReplayStartEvent blk)
    | TraceReplayProgressEvent (TraceReplayProgressEvent blk)
    deriving (Show, Eq)

-- | Add the tip of the Immutable DB to the trace event
--
-- Between the tip of the immutable DB and the point of the starting block,
-- the node could (if it so desired) easily compute a "percentage complete".
decorateReplayTracerWithGoal
  :: Point blk -- ^ Tip of the ImmutableDB
  -> Tracer m (TraceReplayProgressEvent blk)
  -> Tracer m (ReplayGoal blk -> TraceReplayProgressEvent blk)
decorateReplayTracerWithGoal immTip = (($ ReplayGoal immTip) >$<)

-- | Add the block at which a replay started.
--
-- This allows to compute a "percentage complete" when tracing the events.
decorateReplayTracerWithStart
  :: Point blk -- ^ Starting point of the replay
  -> Tracer m (ReplayGoal blk -> TraceReplayProgressEvent blk)
  -> Tracer m (ReplayStart blk -> ReplayGoal blk -> TraceReplayProgressEvent blk)
decorateReplayTracerWithStart start = (($ ReplayStart start) >$<)

-- | Which point the replay started from
newtype ReplayStart blk = ReplayStart (Point blk) deriving (Eq, Show)

-- | Which point the replay is expected to end at
newtype ReplayGoal blk = ReplayGoal (Point blk) deriving (Eq, Show)

-- | Events traced while replaying blocks against the ledger to bring it up to
-- date w.r.t. the tip of the ImmutableDB during initialisation. As this
-- process takes a while, we trace events to inform higher layers of our
-- progress.
data TraceReplayStartEvent blk
  = -- | There were no LedgerDB snapshots on disk, so we're replaying all blocks
    -- starting from Genesis against the initial ledger.
    ReplayFromGenesis
    -- | There was a LedgerDB snapshot on disk corresponding to the given tip.
    -- We're replaying more recent blocks against it.
  | ReplayFromSnapshot
        DiskSnapshot
        (ReplayStart blk) -- ^ the block at which this replay started
  deriving (Generic, Eq, Show)

-- | We replayed the given block (reference) on the genesis snapshot during
-- the initialisation of the LedgerDB. Used during ImmutableDB replay.
data TraceReplayProgressEvent blk =
  ReplayedBlock
    (RealPoint blk)   -- ^ the block being replayed
    [LedgerEvent blk]
    (ReplayStart blk) -- ^ the block at which this replay started
    (ReplayGoal blk)  -- ^ the block at the tip of the ImmutableDB
  deriving (Generic, Eq, Show)
