module Ouroboros.Consensus.MiniProtocol.ChainSync.GenesisDensityGovernor.Trace (
    CheckpointEvent (..)
  , TraceEvent (..)
  ) where

import           Data.Map.Strict (Map)
import           Data.Word (Word64)
import           Ouroboros.Consensus.Block (Header, Point)
import           Ouroboros.Network.AnchoredFragment (AnchoredFragment)

data TraceEvent blk peer =
    KillingPeer peer
  | Checkpoint (CheckpointEvent blk peer)

data CheckpointEvent blk peer = CheckpointEvent {
  densityBounds :: Map peer (AnchoredFragment (Header blk), Bool, Word64, Word64),
  newCandidateTips :: Map peer (Point (Header blk)),
  losingPeers :: [peer],
  loeFrag :: AnchoredFragment (Header blk)
}
