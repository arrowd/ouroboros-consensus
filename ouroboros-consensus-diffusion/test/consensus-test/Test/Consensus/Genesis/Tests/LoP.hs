{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE NumericUnderscores  #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Consensus.Genesis.Tests.LoP (tests) where

import           Data.Functor (($>))
import qualified Ouroboros.Consensus.MiniProtocol.ChainSync.Client as CSClient
import           Ouroboros.Consensus.Util.IOLike (DiffTime,
                     ExceptionInLinkedThread (ExceptionInLinkedThread),
                     Time (Time), fromException)
import qualified Ouroboros.Network.AnchoredFragment as AF
import           Test.Consensus.BlockTree (btTrunk)
import           Test.Consensus.Genesis.Setup
import           Test.Consensus.PeerSimulator.Run (defaultSchedulerConfig,
                     scEnableChainSyncTimeouts, scEnableLoP)
import           Test.Consensus.PeerSimulator.StateView
import           Test.Consensus.PointSchedule
import           Test.Consensus.PointSchedule.Peers (Peers, peersOnlyHonest)
import           Test.Consensus.PointSchedule.Shrinking (shrinkPeerSchedules)
import           Test.Consensus.PointSchedule.SinglePeer (SchedulePoint (..))
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.Orphans.IOLike ()
import           Test.Util.TestEnv (adjustQuickCheckTests)

tests :: TestTree
tests =
  testGroup "LoP" [
    -- | NOTE: Running the test that must _not_ timeout (@prop_smoke False@) takes
    -- significantly more time than the one that does. This is because the former
    -- does all the computation (serving the headers, validating them, serving the
    -- block, validating them) while the former does nothing, because it timeouts
    -- before reaching the last tick of the point schedule.
    adjustQuickCheckTests (`div` 10) $
      testProperty "smoke: must not timeout" (prop_smoke False),
    testProperty "smoke: must timeout" (prop_smoke True)
  ]

prop_smoke :: Bool -> Property
prop_smoke mustTimeout =
  forAllGenesisTest

    (do gt@GenesisTest{gtBlockTree} <- genChains (pure 0)
        let ps = dullSchedule 10 (btTrunk gtBlockTree)
            gt' = gt { gtLoPBucketParams = LoPBucketParams {lbpCapacity=10, lbpRate=1} }
        pure $ gt' $> ps)

    -- NOTE: Crucially, there must not be timeouts for this test.
    (defaultSchedulerConfig {scEnableChainSyncTimeouts = False, scEnableLoP = True})

    shrinkPeerSchedules

    (\_ StateView{svChainSyncExceptions} ->
      case svChainSyncExceptions of
        [] -> not mustTimeout
        [ChainSyncException _pid exn] ->
          case fromException exn of
            -- REVIEW: Where does it get wrapped in 'ExceptionInLinkedThread'?
            -- LeakyBucket is supposed to unwrap it, but maybe somewhere else?
            Just (ExceptionInLinkedThread _ e) | fromException e == Just CSClient.EmptyBucket -> mustTimeout
            _ -> False
        _ -> False
    )

  where
    dullSchedule :: DiffTime -> TestFrag -> Peers PeerSchedule
    dullSchedule _ (AF.Empty _) = error "requires a non-empty block tree"
    dullSchedule timeout (_ AF.:> tipBlock) =
      let offset :: DiffTime = if mustTimeout then 1 else -1
       in peersOnlyHonest $ [
            (Time 0, ScheduleTipPoint tipBlock),
            -- This last point does not matter, it is only here to leave the
            -- connection open (aka. keep the test running) long enough to
            -- pass the timeout by 'offset'.
            (Time (timeout + offset), ScheduleHeaderPoint tipBlock),
            (Time (timeout + offset), ScheduleBlockPoint tipBlock)
            ]
