-- | This module contains functions for generating random point schedules.
module Test.Consensus.PointSchedule.SinglePeer
  ( PeerScheduleParams
  , defaultPeerScheduleParams
  , singleJumpPeerSchedule
  , peerScheduleFromTipPoints
  )
  where

import           Cardano.Slotting.Slot (WithOrigin(At, Origin))
import           Control.Arrow (second)
import           Data.List (mapAccumL)
import           Data.Time.Clock (DiffTime)
import qualified Ouroboros.Network.AnchoredFragment as AF
import           Ouroboros.Network.Block (SlotNo, Tip, tipFromHeader)
import           Ouroboros.Consensus.Block.Abstract (getHeader)
import           Test.Consensus.PointSchedule.SinglePeer.Indices
  ( HeaderPointSchedule (hpsTrunk, hpsBranch)
  , headerPointSchedule
  , singleJumpTipPoints
  , tipPointSchedule
  )
import qualified System.Random.Stateful as R (StatefulGen)
import           Test.Util.TestBlock (Header, TestBlock, tbSlot)

-- | A point in the schedule of a single peer.
data SchedulePoint
  = ScheduleTipPoint (Tip TestBlock)
  | ScheduleHeaderPoint (Header TestBlock)
  | ScheduleBlockPoint TestBlock
  deriving (Eq, Show)

-- | Parameters for generating a schedule for a single peer.
--
-- In the most general form, the caller provides a list of tip points and the
-- schedule is generated by following the given tip points. All headers points
-- and block points are sent eventually, but the points are delayed according
-- to these parameters.
data PeerScheduleParams = PeerScheduleParams
  { pspSlotLength :: DiffTime
    -- | Each of these pairs specifies a range of delays for a point. The
    -- actual delay is chosen uniformly at random from the range.
    --
    -- For tip points, the delay is relative to the slot of the tip point.
  , pspTipDelayInterval :: (DiffTime, DiffTime)
    -- | For header points, the delay is relative to the previous header point
    -- or the tip point that advertises the existence of the header (whichever
    -- happened most recently).
  , pspHeaderDelayInterval :: (DiffTime, DiffTime)
    -- | For block points, the delay is relative to the previous block point or
    -- the header point that advertises the existence of the block (whichever
    -- happened most recently).
  , pspBlockDelayInterval :: (DiffTime, DiffTime)
  }

defaultPeerScheduleParams :: PeerScheduleParams
defaultPeerScheduleParams = PeerScheduleParams
  { pspSlotLength = 20
  , pspTipDelayInterval = (0, 1)
  , pspHeaderDelayInterval = (0.018, 0.021)
  , pspBlockDelayInterval = (0.050, 0.055)
  }

-- | Generate a schedule for a single peer that jumps once to the middle of a
-- sequence of blocks.
--
--  See 'peerScheduleFromTipPoints' for generation of schedules with rollbacks
singleJumpPeerSchedule
  :: R.StatefulGen g m
  => g
  -> PeerScheduleParams
  -> AF.AnchoredFragment TestBlock
  -> m [(DiffTime, SchedulePoint)]
singleJumpPeerSchedule g psp chain = do
    -- generate the tip points
    ixs <- singleJumpTipPoints g 0 (AF.length chain - 1)
    let tipPointBlks = getBlocksFromSortedIndices ixs chain
        tipPointSlots = map tbSlot tipPointBlks
    -- generate the tip point schedule
    ts <- tipPointSchedule g (pspSlotLength psp) (pspTipDelayInterval psp) tipPointSlots
    -- generate the header point schedule
    hpss <- headerPointSchedule g (pspHeaderDelayInterval psp) [(Nothing, zip ts ixs)]
    let hps = concatMap hpsTrunk hpss
    -- generate the block point schedule
    bpss <- headerPointSchedule g (pspBlockDelayInterval psp) [(Nothing, hps)]
    let bps = concatMap hpsTrunk bpss
        tipPointTips =
          zip ts (map (ScheduleTipPoint . tipFromHeader) tipPointBlks)
        hpsHeaders =
          zip
            (map fst hps)
            (map (ScheduleHeaderPoint . getHeader) $ getBlocksFromSortedIndices (map snd hps) chain)
        bpsBlks =
          zip
            (map fst bps)
            (map ScheduleBlockPoint $ getBlocksFromSortedIndices (map snd bps) chain)
    -- merge the schedules
    pure $
      mergeOn fst tipPointTips $
      mergeOn fst hpsHeaders bpsBlks

data IsTrunk = IsTrunk | IsBranch
  deriving (Eq, Show)

-- | @peerScheduleFromTipPoints g params tps trunk branches@ generates a schedule for
-- a single peer that follows the given tip points.
--
-- @tps@ contains the tip points for each fragment.
--
-- @trunk@ is the fragment for the honest chain
--
-- @branches@ contains the fragments for the alternative chains in ascending
-- order of their intersections with the honest chain.
--
peerScheduleFromTipPoints
  :: R.StatefulGen g m
  => g
  -> PeerScheduleParams
  -> [(IsTrunk, [Int])]
  -> AF.AnchoredFragment TestBlock
  -> [AF.AnchoredFragment TestBlock]
  -> m [(DiffTime, SchedulePoint)]
peerScheduleFromTipPoints g psp tipPoints trunk0 branches0 = do
    let (isTrunks, tpSegments) = unzip tipPoints
        tipPointBlks = concat $ indicesToBlocks trunk0 branches0 tipPoints
        tipPointSlots = map tbSlot tipPointBlks
    -- generate the tip point schedule
    ts <- tipPointSchedule g (pspSlotLength psp) (pspTipDelayInterval psp) tipPointSlots
    -- generate the header point schedule
    let tpSchedules = attachTimesToTipPoints ts tpSegments
        intersections = intersectionsAsBlockIndices trunk0 branches0 isTrunks
    hpss <- headerPointSchedule g (pspHeaderDelayInterval psp) $ zip intersections tpSchedules
    -- generate the block point schedule
    let hpsPerBranch = concat
          [ [(Nothing, hpsTrunk hps), (mi, hpsBranch hps)]
          | (mi, hps) <- zip intersections hpss
          ]
    bpss <- headerPointSchedule g (pspBlockDelayInterval psp) hpsPerBranch
    let bpsPerBranch = concat
          [ [(Nothing, hpsTrunk hps), (mi, hpsBranch hps)]
          | (mi, hps) <- zip intersections bpss
          ]
    -- inject tips, headers, and blocks into SchedulePoint
    let tipPointTips =
          zip ts (map (ScheduleTipPoint . tipFromHeader) tipPointBlks)
        hpsHeaders =
          map (second (ScheduleHeaderPoint . getHeader)) $ scheduleIndicesToBlocks trunk0 branches0 hpsPerBranch
        bpsBlks = map (second ScheduleBlockPoint) $ scheduleIndicesToBlocks trunk0 branches0 bpsPerBranch
    -- merge the schedules
    pure $
      mergeOn fst tipPointTips $
      mergeOn fst hpsHeaders bpsBlks

  where
    attachTimesToTipPoints
      :: [DiffTime] -> [[Int]] -> [[(DiffTime, Int)]]
    attachTimesToTipPoints [] [] = []
    attachTimesToTipPoints ts (ixs:ixss) =
      let (ts', rest) = splitAt (length ixs) ts
       in zip ts' ixs : attachTimesToTipPoints rest ixss
    attachTimesToTipPoints _ _ = error "lengths of lists don't match"

    -- | Replaces block indices with the actual blocks
    scheduleIndicesToBlocks
      :: AF.AnchoredFragment TestBlock
      -> [AF.AnchoredFragment TestBlock]
      -> [(Maybe Int, [(DiffTime, Int)])]
      -> [(DiffTime, TestBlock)]
    scheduleIndicesToBlocks trunk branches ((Nothing, s) : ss) =
      let (trunk', blks) = getBlocksWithResidue (map snd s) trunk
       in zip (map fst s) blks ++ scheduleIndicesToBlocks trunk' branches ss
    scheduleIndicesToBlocks trunk (branch:branches) ((Just _, s) : ss) =
      let blks = getBlocksFromSortedIndices (map snd s) branch
       in zip (map fst s) blks ++ scheduleIndicesToBlocks trunk branches ss
    scheduleIndicesToBlocks _  _ [] = []
    scheduleIndicesToBlocks _ _ _ = error "not enough branches"

    indicesToBlocks
      :: AF.AnchoredFragment TestBlock
      -> [AF.AnchoredFragment TestBlock]
      -> [(IsTrunk, [Int])]
      -> [[TestBlock]]
    indicesToBlocks trunk branches ((IsTrunk, s) : ss) =
      let (trunk', blks) = getBlocksWithResidue s trunk
       in blks : indicesToBlocks trunk' branches ss
    indicesToBlocks trunk (branch:branches) ((IsBranch, s) : ss) =
      let blks = getBlocksFromSortedIndices s branch
       in blks : indicesToBlocks trunk branches ss
    indicesToBlocks _  _ [] = []
    indicesToBlocks _ _ _ = error "not enough branches"


-- | Get the block indices of the intersection points of the given chains.
--
-- The branches should be given in ascending order of their intersections with
-- the trunk.
--
-- > intersectionsAsBlockIndices
-- >   :: trunk:AF.AnchoredFragment TestBlock
-- >   -> branches:[AF.AnchoredFragment TestBlock]
-- >   -> isTrunks:[IsTrunk]
-- >   -> {v:[Int] | length v == length isTrunks}
--
intersectionsAsBlockIndices
  :: AF.AnchoredFragment TestBlock
  -> [AF.AnchoredFragment TestBlock]
  -> [IsTrunk]
  -> [Maybe Int]
intersectionsAsBlockIndices _trunk _branches [] = []
intersectionsAsBlockIndices trunk0 branches isTrunks =
    let anchors = map fragmentAnchor branches
     in snd $ mapAccumL findIntersection (0, trunk0, anchors) isTrunks
  where
    findIntersection
      :: (Int, AF.AnchoredFragment TestBlock, [SlotNo])
      -> IsTrunk
      -> ((Int, AF.AnchoredFragment TestBlock, [SlotNo]), Maybe Int)
    findIntersection acc@(_, (_ AF.:< _), _) IsTrunk =
      (acc, Nothing)
    findIntersection (n, trunk@(b AF.:< rest), anchors@(anchor:as)) IsBranch =
      if anchor == (-1) then
        ((n, trunk, as), Just (-1))
      else if tbSlot b == anchor then
        ((n, trunk, as), Just n)
      else
        findIntersection (n+1, rest, anchors) IsBranch
    findIntersection _ _ = error "findIntersection: empty fragment"

    fragmentAnchor :: AF.AnchoredFragment TestBlock -> SlotNo
    fragmentAnchor f = case AF.anchorToSlotNo (AF.anchor f) of
      At s -> s
      Origin -> -1

-- | Get the blocks at the given offsets from the given chain.
--
-- PRECONDITION: The offsets are in ascending order, there are no duplicates
-- and do not exceed the length of the chain.
--
getBlocksFromSortedIndices :: [Int] -> AF.AnchoredFragment TestBlock -> [TestBlock]
getBlocksFromSortedIndices ixs = snd . getBlocksWithResidue ixs

getBlocksWithResidue
  :: [Int] -> AF.AnchoredFragment TestBlock -> (AF.AnchoredFragment TestBlock, [TestBlock])
getBlocksWithResidue [] chain = (chain, [])
getBlocksWithResidue ixs0 chain = go 0 ixs0 chain []
  where
    go
      :: Int
      -> [Int]
      -> AF.AnchoredFragment TestBlock
      -> [TestBlock]
      -> (AF.AnchoredFragment TestBlock, [TestBlock])
    go cur iixs@(i:ixs) (x AF.:< s) acc =
      if cur == i then
        go (cur + 1) ixs s (x:acc)
      else
        go (cur + 1) iixs s acc
    go _ [] rest acc = (rest, reverse acc)
    go _  _  AF.Empty{} _ = error "offsets exceed chain length"

-- | Merge two sorted lists.
--
-- PRECONDITION: The lists are sorted.
--
mergeOn :: Ord b => (a -> b) -> [a] -> [a] -> [a]
mergeOn _f [] ys = ys
mergeOn _f xs [] = xs
mergeOn f xxs@(x:xs) yys@(y:ys) =
    if f x <= f y
      then x : mergeOn f xs yys
      else y : mergeOn f xxs ys