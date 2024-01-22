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

tests :: TestTree
tests = testGroup "HeaderValidation"
  [ testGroup "validateIfCheckpoint"
    [ testProperty "non-checkpoints are ignored" prop_validateIfCheckpoint_nonCheckpoint
    , testProperty "checkpoint matches should be accepted" prop_validateIfCheckpoint_checkpoint_matches
    , testProperty "checkpoint mismatches should be rejected" prop_validateIfCheckpoint_checkpoint_mismatches
    ]
  ]

-- | Make a test block from the length of the hash and the word64
-- to create it.
mkTestBlock :: QC.NonNegative Int -> Word64 -> TestBlock
mkTestBlock (QC.NonNegative n) h =
    successorBlockWithPayload
      (TestHash $ h :| replicate (n-1) h)
      (fromIntegral n)
      ()

-- | Like validateIfCheckpoint, but takes a list of blocks to use as
-- checkpoints.
validateIfCheckpointBlocks
  :: [TestBlock] -> TestBlock -> Either (HeaderEnvelopeError TestBlock) ()
validateIfCheckpointBlocks xs x =
    runExcept $
      validateIfCheckpoint
        (CheckpointsMap $ Map.fromList [ (blockNo b, blockHash b) | b <- xs])
        (getHeader x)

prop_validateIfCheckpoint_nonCheckpoint
  :: [QC.NonNegative Int] -> QC.NonNegative Int -> QC.Property
prop_validateIfCheckpoint_nonCheckpoint xs x0 =
    let
      blks = map (`mkTestBlock` 0) $ filter (/= x0) xs
     in
      case validateIfCheckpointBlocks blks (mkTestBlock x0 0) of
        Left _  ->
          QC.counterexample "checkpoint validation should not fail on other blocks than checkpoints" $
          QC.property False
        Right _ -> QC.property True

prop_validateIfCheckpoint_checkpoint_matches
  :: [QC.NonNegative Int] -> QC.NonNegative Int -> QC.Property
prop_validateIfCheckpoint_checkpoint_matches xs x =
    let
      blks = map (`mkTestBlock` 0) (x:xs)
     in
      case validateIfCheckpointBlocks blks (mkTestBlock x 0) of
        Left _  ->
          QC.counterexample "checkpoint matches should be accepted" $
          QC.property False
        Right _ -> QC.property True

prop_validateIfCheckpoint_checkpoint_mismatches
  :: [QC.NonNegative Int] -> QC.NonNegative Int -> QC.Property
prop_validateIfCheckpoint_checkpoint_mismatches xs x =
    let
      blks = map (`mkTestBlock` 0) (x:xs)
     in
      case validateIfCheckpointBlocks blks (mkTestBlock x 1) of
        Left _  -> QC.property True
        Right _ ->
          QC.counterexample "checkpoint mismatches should be rejected" $
          QC.property False
