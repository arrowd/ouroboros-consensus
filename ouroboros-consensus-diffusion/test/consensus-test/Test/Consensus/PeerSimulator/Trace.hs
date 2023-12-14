{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE NamedFieldPuns     #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeFamilies       #-}

-- | Helpers for tracing used by the peer simulator.
module Test.Consensus.PeerSimulator.Trace (
    mkCdbTracer
  , mkChainSyncClientTracer
  , mkGenesisDensityGovernorTracer
  , prettyTime
  , traceLinesWith
  , traceUnitWith
  ) where

import           Control.Tracer (Tracer (Tracer), traceWith)
import           Data.List (intercalate)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Time.Clock (diffTimeToPicoseconds)
import           Data.Word (Word64)
import           Ouroboros.Consensus.MiniProtocol.ChainSync.Client
                     (TraceChainSyncClientEvent (..))
import           Ouroboros.Consensus.MiniProtocol.ChainSync.GenesisDensityGovernor.Trace as GDG
import qualified Ouroboros.Consensus.Storage.ChainDB.Impl as ChainDB.Impl
import           Ouroboros.Consensus.Storage.ChainDB.Impl.Types
                     (SelectionChangedInfo (..), TraceAddBlockEvent (..))
import           Ouroboros.Consensus.Util.Condense (condense)
import           Ouroboros.Consensus.Util.IOLike (IOLike, MonadMonotonicTime,
                     Time (Time), getMonotonicTime)
import           Ouroboros.Network.AnchoredFragment (AnchoredFragment,
                     castPoint)
import           Test.Consensus.PointSchedule (PeerId)
import           Test.Util.TersePrinting (terseHFragment, tersePoint,
                     terseRealPoint)
import           Test.Util.TestBlock (Header, TestBlock)
import           Text.Printf (printf)

mkCdbTracer ::
  IOLike m =>
  Tracer m String ->
  Tracer m (ChainDB.Impl.TraceEvent TestBlock)
mkCdbTracer tracer =
  Tracer $ \case
    ChainDB.Impl.TraceAddBlockEvent event ->
      case event of
        AddedToCurrentChain _ SelectionChangedInfo {newTipPoint} _ _ -> do
          trace "Added to current chain"
          trace $ "New tip: " ++ terseRealPoint newTipPoint
        SwitchedToAFork _ SelectionChangedInfo {newTipPoint} _ newFragment -> do
          trace "Switched to a fork"
          trace $ "New tip: " ++ terseRealPoint newTipPoint
          trace $ "New fragment: " ++ terseHFragment newFragment
        StoreButDontChange block -> do
          trace "Did not add block due to LoE"
          trace $ "Block: " ++ terseRealPoint block
        _ -> pure ()
    _ -> pure ()
  where
    trace = traceUnitWith tracer "ChainDB"

mkChainSyncClientTracer ::
  IOLike m =>
  Tracer m String ->
  Tracer m (TraceChainSyncClientEvent TestBlock)
mkChainSyncClientTracer tracer =
  Tracer $ \case
    TraceRolledBack point ->
      trace $ "Rolled back to: " ++ tersePoint point
    TraceFoundIntersection point _ourTip _theirTip ->
      trace $ "Found intersection at: " ++ tersePoint point
    _ -> pure ()
  where
    trace = traceUnitWith tracer "ChainSyncClient"

mkGenesisDensityGovernorTracer ::
  Monad m =>
  Tracer m String ->
  Tracer m (GDG.TraceEvent TestBlock PeerId)
mkGenesisDensityGovernorTracer tracer =
  Tracer $ \case
    KillingPeer peer -> trace $ "Killed peer: " ++ condense peer
    Checkpoint (CheckpointEvent{densityBounds, newCandidateTips, losingPeers, loeFrag}) -> do
      trace $ "Density bounds: " ++ showPeers (showBounds <$> densityBounds)
      trace $ "New candidate tips: " ++ showPeers (tersePoint . castPoint <$> newCandidateTips)
      trace $ "Losing peers: " ++ show losingPeers
      trace $ "LoE fragment: " ++ terseHFragment loeFrag
  where
    trace = traceUnitWith tracer "GenesisDensityGovernor"

    showBounds :: (AnchoredFragment (Header blk), Bool, Word64, Word64) -> String
    showBounds (_, more, lower, upper) = show lower ++ "/" ++ show upper ++ "[" ++ (if more then "+" else " ") ++ "]"

    showPeers :: Map PeerId String -> String
    showPeers = intercalate ", " . fmap (\(peer, v) -> condense peer ++ " -> " ++ v) . Map.toList

prettyTime :: MonadMonotonicTime m => m String
prettyTime = do
  Time time <- getMonotonicTime
  let ps = diffTimeToPicoseconds time
      milliseconds = (ps `div` 1_000_000_000) `mod` 1_000
      seconds = (ps `div` 1_000_000_000_000) `rem` 60
      minutes = (ps `div` 1_000_000_000_000) `quot` 60
  pure $ printf "%02d:%02d.%03d" minutes seconds milliseconds

-- | Trace using the given tracer, printing the current time (typically the time
-- of the simulation) and the unit name.
traceUnitWith :: Tracer m String -> String -> String -> m ()
traceUnitWith tracer unit msg =
  traceWith tracer $ printf "%s | %s" unit msg

traceLinesWith ::
  Tracer m String ->
  [String] ->
  m ()
traceLinesWith tracer = traceWith tracer . unlines
