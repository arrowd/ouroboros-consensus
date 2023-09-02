{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Ouroboros.Consensus.MiniProtocol.ChainSync.GenesisDensityGovernor (run) where

import           Control.Monad (guard)
import           Data.Containers.ListUtils (nubOrd)
import           Data.Foldable
import qualified Data.List as L
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Void
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.MiniProtocol.ChainSync.Client
                     (ChainSyncClientHandle (..))
import           Ouroboros.Consensus.Storage.ChainDB.API (ChainDB)
import qualified Ouroboros.Consensus.Storage.ChainDB.API as ChainDB
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.STM (blockUntilChanged)
import           Ouroboros.Network.AnchoredFragment (AnchoredFragment)
import qualified Ouroboros.Network.AnchoredFragment as AF
import           Ouroboros.Network.Block

-- | Run the Genesis disconnection logic.
--
--   * Maintain and update the LoE.
--
--   * Disconnect from peers with inferior density.
run ::
     forall m blk peer.
     ( IOLike m
     , Ord peer
     , LedgerSupportsProtocol blk
     )
  => ChainDB m blk
  -> TopLevelConfig blk
  -> STM m (Map peer (StrictTVar m (AnchoredFragment (Header blk))))
  -> STM m (Map peer (ChainSyncClientHandle m blk))
  -> m Void
run chainDB cfg getCandidates getHandles = go Map.empty
  where
    go oldCandidateTips = do
        (killPeers, newCandidateTips) <- atomically $ do
          (candidates, newCandidateTips) <-
            blockUntilChanged (Map.map AF.headPoint) oldCandidateTips $
              -- TODO: reading from n TVars in one transaction has complexity
              -- O(n²).
              getCandidates >>= traverse readTVar
          curChain <- ChainDB.getCurrentChain chainDB
          handles <- getHandles
          immutableLedgerSt <- ChainDB.getImmutableLedger chainDB

          let immutableTip = AF.anchorPoint curChain
              splitAfterImmutableTip frag =
                snd <$> AF.splitAfterPoint frag immutableTip
              immutableTipSuffixes =
                -- If a ChainSync client's candidate forks off before the
                -- immutable tip, then this transaction is currently winning an
                -- innocuous race versus the thread that will fatally raise
                -- 'InvalidIntersection' within that ChainSync client, so it's
                -- sound to pre-emptively discard their candidate from this
                -- 'Map' via 'mapMaybe'.
                Map.mapMaybe splitAfterImmutableTip candidates

              (loeFrag, candidateSuffixes) =
                stripCommonPrefix (AF.anchor curChain) immutableTipSuffixes

              loeIntersectionSlot = AF.headSlot loeFrag
              -- exclusive last slot in the Genesis window
              endOfGenesisWindow =
                  succWithOrigin loeIntersectionSlot + SlotNo sgen
                where
                  GenesisWindow sgen =
                    computeGenesisWindow
                      (configLedger cfg)
                      -- TODO: use a forecasted ledger view for the intersection
                      -- slot (tip of LoE frag).
                      (ledgerState immutableLedgerSt)

              dropBeyondGenesisWindow =
                AF.takeWhileOldest ((< endOfGenesisWindow) . blockSlot)
              competingFrags =
                Map.map dropBeyondGenesisWindow candidateSuffixes

          -- update LoE frag such that ChainSel can progress
          ChainDB.setLoEFrag chainDB loeFrag
          -- TODO: this should probably also somehow "trigger" chain selection
          -- in case the LoE didn't move for some time (also see comment about
          -- "prompt reprocessing" in ChainSel).

          theirTips <-
            flip Map.traverseWithKey competingFrags $ \peer _ ->
              cschTheirTip (handles Map.! peer)

{- TODO: convert this scribble into a useful explanatory diagram, illustrating the
         density calculation below

            |--------|

    frag1: A - B - C - D - ...            <- exact
             \
    frag2:     E           claimed tip: E <- exact

-}

          let densityBounds = Map.fromList $ do
                (peer, frag) <- Map.toList competingFrags
                let theirTip = theirTips Map.! peer
                    candidateSuffix = candidateSuffixes Map.! peer
                    lowerBound = fromIntegral $ AF.length frag
                    unresolvedSlotsLB =
                      succWithOrigin $ AF.headSlot frag
                    unresolvedSlotsUB =
                        endOfGenesisWindow `min` claimedTip
                      where
                        claimedTip = succWithOrigin $ getTipSlotNo theirTip
                    hasBlockAfterGenesisWindow =
                      -- Note that if
                      -- > NotOrigin s = AF.headSlot candidateSuffix
                      -- this check is equivalent to
                      -- > s >= endOfGenesisWindow
                        succWithOrigin (AF.headSlot candidateSuffix)
                      > endOfGenesisWindow
                    upperBound =
                        lowerBound
                      + if hasBlockAfterGenesisWindow
                        then 0
                        else unSlotNo (intervalLength unresolvedSlotsLB unresolvedSlotsUB)
                pure (peer, (frag, lowerBound, upperBound))

              killPeers =
                  for_ losingPeers $ \peer -> cschKill (handles Map.! peer)
                where
                  losingPeers = nubOrd $ do
                    (peer0 , (frag0, _  , ub0)) <- Map.toList densityBounds
                    (_peer1, (frag1, lb1, _  )) <- Map.toList densityBounds
                    -- ensure that the two peer fragments don't share any
                    -- headers after the LoE
                    guard $ AF.lastPoint frag0 /= AF.lastPoint frag1
                    -- peer1 offers more than k blocks
                    guard $ lb1 > k
                    -- peer1 definitely has higher density than peer0
                    guard $ lb1 > ub0
                    pure peer0

          pure (killPeers, newCandidateTips)

        killPeers

        go newCandidateTips

    SecurityParam k = configSecurityParam cfg

    -- Length of an interval with inclusive lower bound @a@ and exclusive upper
    -- bound @b@.
    intervalLength a b
      | a <= b    = b - a
      | otherwise = 0

{-

more TODO:

 - we don't yet check that the header fragments contain no blocks from the
   future (will likely be fixed by efforts not directly related to Genesis)

 - idea by Torsten: seems like it is enough to run this logic whenever we add a
   block to the ChainDB (instead of having yet another background thread)

-}

-- | Strip the common prefix of multiple fragments.
--
-- PRECONDITION: all fragments have the given anchor as their anchor.
--
-- TODO: move elsewhere
stripCommonPrefix ::
     forall f blk.
     (Functor f, Foldable f, HasHeader blk) -- TODO: this uses the lazy 'map' for 'Map'...
  => AF.Anchor blk
  -> f (AnchoredFragment blk)
  -> (AnchoredFragment blk, f (AnchoredFragment blk))
stripCommonPrefix sharedAnchor frags =
    -- TODO assert precondition
    (commonPrefix, splitAfterCommonPrefix <$> frags)
  where
    -- Return the common prefix of two fragments with the same anchor
    -- 'sharedAnchor'.
    computeCommonPrefix ::
         AnchoredFragment blk
      -> AnchoredFragment blk
      -> AnchoredFragment blk
    computeCommonPrefix frag1 frag2 = case AF.intersect frag1 frag2 of
      Just (cp, _, _, _) -> cp
      Nothing            -> error "unreachable"

    commonPrefix
      | null frags = AF.Empty sharedAnchor
      -- TODO use Foldable1 once all our GHCs support it
      | otherwise = L.foldl1' computeCommonPrefix (toList frags)

    splitAfterCommonPrefix frag =
      case AF.splitAfterPoint frag (AF.headPoint commonPrefix) of
        Just (_, afterCommonPrefix) -> afterCommonPrefix
        Nothing                     -> error "unreachable"