{-# LANGUAGE ConstraintKinds          #-}
{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE DeriveGeneric            #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE PolyKinds                #-}
{-# LANGUAGE QuantifiedConstraints    #-}
{-# LANGUAGE RankNTypes               #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications         #-}
{-# LANGUAGE TypeFamilies             #-}
{-# LANGUAGE UndecidableInstances     #-}

-- | Arguments for LedgerDB initialization.
module Ouroboros.Consensus.Storage.LedgerDB.Impl.Args (
    LedgerDbArgs (..)
  , defaultArgs
    -- * Tracing
  , ReplayGoal (..)
  , ReplayStart (..)
  , TraceLedgerDBEvent (..)
  , TraceReplayEvent (..)
  , TraceReplayProgressEvent (..)
  , TraceReplayStartEvent (..)
  , decorateReplayTracerWithGoal
  , decorateReplayTracerWithStart
    --
  , FlavorImplSpecificTrace (..)
  , LedgerDbFlavorArgs (..)
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
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.Args as V1
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore as V1
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.Args as V2
import           Ouroboros.Consensus.Util.Args
import           System.FS.API

{-------------------------------------------------------------------------------
  Arguments
-------------------------------------------------------------------------------}

-- | Arguments required to initialize a LedgerDB.
type LedgerDbArgs ::
     (Type -> Type)
  -> (Type -> Type)
  -> Type
  -> Type
data LedgerDbArgs f m blk = LedgerDbArgs {
      lgrSnapshotInterval :: SnapshotInterval
    , lgrGenesis          :: HKD f (m (ExtLedgerState blk ValuesMK))
    , lgrHasFS            :: HKD f (SomeHasFS m)
    , lgrConfig           :: HKD f (LedgerDbCfg (ExtLedgerState blk))
    , lgrTracer           :: Tracer m (TraceLedgerDBEvent blk)
    , lgrFlavorArgs       :: LedgerDbFlavorArgs m
    }

-- | Default arguments
defaultArgs ::
     ( Applicative m
     )
  => Incomplete LedgerDbArgs m blk
defaultArgs = LedgerDbArgs {
      lgrSnapshotInterval = DefaultSnapshotInterval
    , lgrGenesis          = NoDefault
    , lgrHasFS            = NoDefault
    , lgrConfig           = NoDefault
    , lgrTracer           = nullTracer
    , lgrFlavorArgs       = LedgerDbFlavorArgsV1 V1.defaultLedgerDbFlavorArgs
    }

data LedgerDbFlavorArgs m where
  LedgerDbFlavorArgsV1 :: V1.LedgerDbFlavorArgs m -> LedgerDbFlavorArgs m
  LedgerDbFlavorArgsV2 :: V2.LedgerDbFlavorArgs m -> LedgerDbFlavorArgs m

{-------------------------------------------------------------------------------
  Tracing
-------------------------------------------------------------------------------}

data FlavorImplSpecificTrace =
    FlavorImplSpecificTraceV1 V1.FlavorImplSpecificTrace
  | FlavorImplSpecificTraceV2 V2.FlavorImplSpecificTrace
  deriving (Show, Eq)

data TraceLedgerDBEvent blk =
      LedgerDBSnapshotEvent   !(TraceSnapshotEvent blk)
    | LedgerReplayEvent       !(TraceReplayEvent blk)
    | LedgerDBForkerEvent     !TraceForkerEventWithKey
    | LedgerDBFlavorImplEvent !FlavorImplSpecificTrace
  deriving (Generic, Show, Eq)

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
