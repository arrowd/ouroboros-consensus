{-# LANGUAGE NamedFieldPuns #-}

module Test.Consensus.PeerSimulator.StateView (
    ChainSyncException (..)
  , StateView (..)
  , StateViewTracers (..)
  , defaultStateViewTracers
  , snapshotStateView
  , svImmutableDbTip
  , svSelectedChainTip
  ) where

import           Control.Tracer (Tracer)
import qualified Data.List.NonEmpty as NonEmpty
import           Ouroboros.Consensus.Storage.ChainDB (ChainDB)
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import           Ouroboros.Consensus.Util.Condense (Condense (condense))
import           Ouroboros.Consensus.Util.IOLike (IOLike, SomeException,
                     atomically)
import qualified Ouroboros.Network.AnchoredFragment as AF
import           Ouroboros.Network.Block (Tip, tipFromHeader)
import           Test.Consensus.PeerSimulator.Trace (terseFragH)
import           Test.Consensus.PointSchedule (PeerId,
                     PointSchedule (PointSchedule), TestFragH, ticks)
import           Test.Util.TestBlock (TestBlock, testHeader)
import           Test.Util.Tracer (recordingTracerTVar)

-- | A record to associate an exception thrown by the ChainSync
-- thread with the peer that it was running for.
data ChainSyncException = ChainSyncException
       { csePeerId    :: PeerId
       , cseException :: SomeException
       }
    deriving Show

-- | A state view is a partial view of the state of the whole peer simulator.
-- This includes information about the part of the code that is being tested
-- (for instance the fragment that is selected by the ChainDB) but also
-- information about the mocked peers (for instance the exceptions raised in the
-- mocked ChainSync server threads).
data StateView = StateView {
    svPointSchedule       :: PointSchedule,
    svSelectedChain       :: TestFragH,
    svChainSyncExceptions :: [ChainSyncException]
  }
  deriving Show

-- | Tip of the StateView's selection.
svSelectedChainTip :: StateView -> Tip TestBlock
svSelectedChainTip = either AF.anchorToTip (tipFromHeader . testHeader) . AF.head . svSelectedChain

-- | Tip of the StateView's immutable database.
svImmutableDbTip :: StateView -> Tip TestBlock
svImmutableDbTip = AF.anchorToTip . AF.anchor . svSelectedChain

instance Condense StateView where
  condense StateView {svSelectedChain, svChainSyncExceptions, svPointSchedule=PointSchedule{ticks}} =
    "SelectedChain: " ++ terseFragH svSelectedChain ++ "\n"
    ++ "ChainSyncExceptions: " ++ show svChainSyncExceptions ++ "\n"
    ++ "PointSchedule:\n"
    ++ (if length ticks == 1
          then ""
          else "  ..." ++ show (length ticks - 1) ++ " more ticks...\n")
    ++ "  " ++ condense (NonEmpty.last ticks)

-- | State view tracers are a lightweight mechanism to record information that
-- can later be used to produce a state view. This mechanism relies on
-- contra-tracers which we already use in a pervasives way.
data StateViewTracers m = StateViewTracers {
    svtChainSyncExceptionsTracer :: Tracer m ChainSyncException
  , svtGetChainSyncExceptions    :: m [ChainSyncException]
  }

-- | Make default state view tracers. The tracers are all freshly initialised
-- and contain no information.
defaultStateViewTracers ::
  IOLike m =>
  m (StateViewTracers m)
defaultStateViewTracers = do
  (svtChainSyncExceptionsTracer, svtGetChainSyncExceptions) <- recordingTracerTVar
  pure StateViewTracers {svtChainSyncExceptionsTracer, svtGetChainSyncExceptions}

-- | Use the state view tracers as well as some extra information to produce a
-- state view. This mostly consists in reading and storing the current state of
-- the tracers.
snapshotStateView ::
  IOLike m =>
  StateViewTracers m ->
  PointSchedule ->
  ChainDB m TestBlock ->
  m StateView
snapshotStateView StateViewTracers{svtGetChainSyncExceptions} svPointSchedule chainDb = do
  svChainSyncExceptions <- svtGetChainSyncExceptions
  svSelectedChain <- atomically $ ChainDB.getCurrentChain chainDb
  pure StateView {svPointSchedule, svSelectedChain, svChainSyncExceptions}
