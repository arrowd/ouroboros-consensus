{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
-- | Property tests for header validations
module Test.Consensus.HeaderValidation (tests) where

import           Control.Monad.Except (runExcept)
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.Map.Strict as Map
import           Data.Word (Word64)
import           Ouroboros.Consensus.Block.Abstract
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.HeaderValidation
import qualified Test.QuickCheck as QC
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.TestBlock
import           Data.SOP.BasicFunctors
import           Data.SOP.Strict
import qualified Data.SOP.Telescope as Telescope
import           Ouroboros.Consensus.HardFork.Combinator
import           Ouroboros.Consensus.HardFork.Combinator.Embed.Unary
import           Ouroboros.Consensus.TypeFamilyWrappers
import           Ouroboros.Consensus.HardFork.Combinator.Degenerate
import           Ouroboros.Consensus.HardFork.Combinator.State.Types
import           Ouroboros.Consensus.HardFork.History.Summary

tests :: TestTree
tests = testGroup "HeaderValidation"
  [ testGroup "validateIfCheckpoint"
    [ testProperty "non-checkpoints are ignored" prop_validateIfCheckpoint_nonCheckpoint
    , testProperty "checkpoint matches should be accepted" prop_validateIfCheckpoint_checkpoint_matches
    , testProperty "checkpoint mismatches should be rejected" prop_validateIfCheckpoint_checkpoint_mismatches
    ]
  ]

type HFTestBlock = HardForkBlock '[TestBlock]

-- | Make a test block from the length of the hash and the Word64
-- to create it.
mkHFTestBlock :: QC.NonNegative Int -> Word64 -> HFTestBlock
mkHFTestBlock (QC.NonNegative n) h =
    DegenBlock $ successorBlockWithPayload
      (TestHash $ h :| replicate n h)
      (fromIntegral n + 1)
      ()

-- | Like 'validateHeader', but takes a list of blocks to use as
-- checkpoints.
validateHeaderBlocks ::
     [HFTestBlock]
  -> HFTestBlock
  -> Either (HeaderError HFTestBlock) (HeaderState HFTestBlock)
validateHeaderBlocks xs x@(DegenBlock xtb) =
    let
      checkpoints = CheckpointsMap $ Map.fromList [ (blockNo b, blockHash b) | b <- xs]
      cfg = (inject singleNodeTestConfig) {topLevelConfigCheckpoints = checkpoints}
      h = getHeader x
      -- Chose the header state carefully so it doesn't cause validation to fail
      annTip =
        case headerPrevHash (getHeader xtb) of
          GenesisHash -> Origin
          BlockHash hsh ->
            NotOrigin $ AnnTip
              (blockSlot x - 1)
              (blockNo x - 1)
              (DegenTipInfo hsh)
      depState = initHardForkState (WrapChainDepState ())
      WrapLedgerView ledgerView = inject (WrapLedgerView ())
      headerState =
        tickHeaderState
          (topLevelConfigProtocol cfg)
          ledgerView
          0
          (HeaderState annTip depState)
     in
      runExcept $
        validateHeader cfg ledgerView h headerState

prop_validateIfCheckpoint_nonCheckpoint
  :: [QC.NonNegative Int] -> QC.NonNegative Int -> QC.Property
prop_validateIfCheckpoint_nonCheckpoint xs x0 =
    let
      blks = map (`mkHFTestBlock` 0) $ filter (/= x0) xs
     in
      case validateHeaderBlocks blks (mkHFTestBlock x0 0) of
        Left e ->
          QC.counterexample "checkpoint validation should not fail on other blocks than checkpoints" $
          QC.counterexample (show e) $
          QC.property False
        Right _ -> QC.property True

prop_validateIfCheckpoint_checkpoint_matches
  :: [QC.NonNegative Int] -> QC.NonNegative Int -> QC.Property
prop_validateIfCheckpoint_checkpoint_matches xs x =
    let
      blks = map (`mkHFTestBlock` 0) (x:xs)
     in
      case validateHeaderBlocks blks (mkHFTestBlock x 0) of
        Left e  ->
          QC.counterexample "checkpoint matches should be accepted" $
          QC.counterexample (show e) $
          QC.property False
        Right _ -> QC.property True

prop_validateIfCheckpoint_checkpoint_mismatches
  :: [QC.NonNegative Int] -> QC.NonNegative Int -> QC.Property
prop_validateIfCheckpoint_checkpoint_mismatches xs x =
    let
      blks = map (`mkHFTestBlock` 0) (x:xs)
     in
      case validateHeaderBlocks blks (mkHFTestBlock x 1) of
        Left (HeaderEnvelopeError CheckpointMismatch{})  -> QC.property True
        Left e  ->
          QC.counterexample "checkpoint mismatches should be rejected with CheckpointMismatched" $
          QC.counterexample (show e) $
          QC.property False
        Right _ ->
          QC.counterexample "checkpoint mismatches should be rejected" $
          QC.property False
