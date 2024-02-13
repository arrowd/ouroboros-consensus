{-# LANGUAGE LambdaCase     #-}
{-# LANGUAGE NamedFieldPuns #-}

module Test.Ouroboros.Consensus.ChainGenerator.Tests.Honest (
    -- * Re-use
    TestHonest (TestHonest, testAsc, testRecipe, testRecipe')
  , unlines'
    -- * Tests
  , tests
  ) where

import qualified Control.Exception as IO (evaluate)
import qualified Control.Monad.Except as Exn
import           Data.Functor ((<&>))
import           Data.Functor.Identity (runIdentity)
import           Data.List (intercalate)
import           Data.Proxy (Proxy (Proxy))
import qualified System.Random as R
import qualified System.Timeout as IO (timeout)
import qualified Test.Ouroboros.Consensus.ChainGenerator.Honest as H
import           Test.Ouroboros.Consensus.ChainGenerator.Params (Asc,
                     Delta (Delta), Kcp (Kcp), Len (Len), Sfor (Sfor),
                     Sgen (Sgen), genAsc, genKSSD)
import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Extras (sized1, unsafeMapSuchThatJust)
import           Test.QuickCheck.Random (QCGen)
import qualified Test.Tasty as TT
import qualified Test.Tasty.QuickCheck as TT

-----

tests :: [TT.TestTree]
tests = [
    TT.testProperty "prop_honestChain" prop_honestChain
  ,
    TT.testProperty "prop_honestChainMutation" prop_honestChainMutation
  ]

-----

data TestHonest = TestHonest {
    testAsc     :: !Asc
  ,
    testRecipe  :: !H.HonestRecipe
  ,
    testRecipe' :: !H.SomeCheckedHonestRecipe
  }
  deriving (Read, Show)

instance QC.Arbitrary TestHonest where
    arbitrary = do
        testAsc <- genAsc
        testRecipe <- H.genHonestRecipe

        testRecipe' <- case Exn.runExcept $ H.checkHonestRecipe testRecipe of
            Left e  -> error $ "impossible! " <> show (testRecipe, e)
            Right x -> pure x

        pure TestHonest {
            testAsc
          ,
            testRecipe
          ,
            testRecipe'
          }

-- | No seed exists such that each 'H.checkHonestChain' rejects the result of 'H.uniformTheHonestChain'
prop_honestChain :: TestHonest -> QCGen -> QC.Property
prop_honestChain testHonest testSeed = runIdentity $ do
    H.SomeCheckedHonestRecipe Proxy Proxy recipe' <- pure testRecipe'

    let sched = H.uniformTheHonestChain (Just testAsc) recipe' testSeed

    QC.counterexample (unlines' $ H.prettyChainSchema sched "H") <$> do
        pure $ case Exn.runExcept $ H.checkHonestChain testRecipe sched of
            Right () -> QC.property ()
            Left e   -> case e of
                H.BadCount{}      -> QC.counterexample (show e) False
                H.BadLength{}     -> QC.counterexample (show e) False
                H.BadSgenWindow v ->
                    let str = case v of
                            H.SgenViolation {
                                H.sgenvWindow = win
                              } -> H.prettyWindow win "SCGV"
                    in
                        id
                      $ QC.counterexample str
                      $ QC.counterexample (show e)
                      $ False
  where
    TestHonest {
        testAsc
      ,
        testRecipe
      ,
        testRecipe'
      } = testHonest

-- 'unlines' adds a trailing newline, this function never does
unlines' :: [String] -> String
unlines' = intercalate "\n"

-----

-- | A mutation that minimally increases the threshold density of an 'H.HonestRecipe''s SCG constraint
data HonestMutation =
    -- | Increasing 'Kcp' by one increases the SCG numerator
    HonestMutateKcp
  |
    -- | Decreasing 'Sgen' by one decreases the SCG denominator
    HonestMutateSgen
  deriving (Eq, Read, Show)

data TestHonestMutation =
    TestHonestMutation
        !H.HonestRecipe
        !H.SomeCheckedHonestRecipe
        !HonestMutation
  deriving (Read, Show)

mutateHonest :: H.HonestRecipe -> HonestMutation -> H.HonestRecipe
mutateHonest recipe mut =
    H.HonestRecipe (Kcp k') (Sgen sgen') (Sfor sfor') (Delta d') len
  where
    H.HonestRecipe (Kcp k) (Sgen sgen) (Sfor sfor) (Delta d) len = recipe

    (k', sgen', sfor', d') = case mut of
        HonestMutateKcp  -> (k + 1, sgen,     sfor,     d    )
        HonestMutateSgen -> (k,     sgen - 1, sfor - 1, d    )

instance QC.Arbitrary TestHonestMutation where
    arbitrary = sized1 $ \sz -> unsafeMapSuchThatJust $ do
        (kcp, Sgen sgen, Sfor sfor, delta) <- genKSSD
        l <- (+ sgen) <$> QC.choose (0, 5 * sz)

        let testRecipe = H.HonestRecipe kcp (Sgen sgen) (Sfor sfor) delta (Len l)

        testRecipe' <- case Exn.runExcept $ H.checkHonestRecipe testRecipe of
            Left e  -> error $ "impossible! " <> show (testRecipe, e)
            Right x -> pure x

        mut <- QC.elements [HonestMutateKcp, HonestMutateSgen]

        pure $ case Exn.runExcept $ H.checkHonestRecipe $ mutateHonest testRecipe mut of
            Left{}  -> Nothing
            Right{} -> Just $ TestHonestMutation testRecipe testRecipe' mut

-- | There exists a seed such that each 'TestHonestMutation' causes
-- 'H.checkHonestChain' to reject the result of 'H.uniformTheHonestChain'
prop_honestChainMutation :: TestHonestMutation -> QCGen -> QC.Property
prop_honestChainMutation testHonestMut testSeedsSeed0 = QC.ioProperty $ do
    H.SomeCheckedHonestRecipe Proxy Proxy recipe' <- pure someRecipe'

    -- we're willing to wait up to 500ms to find a failure for each 'TestHonestMutation'
    IO.timeout
        (5 * 10^(5::Int))
        (IO.evaluate $ go recipe' testSeedsSeed0) <&> \case
            Nothing   -> False   -- did not find a failure caused by the mutation
            Just bool -> bool
  where
    TestHonestMutation recipe someRecipe' mut = testHonestMut

    mutatedRecipe = mutateHonest recipe mut

    go recipe' testSeedsSeed =
        let -- TODO is this a low quality random stream? Why is there no @'R.Random' 'QCGen'@ instance?
            (testSeed, testSeedsSeed') = R.split testSeedsSeed

            sched = H.uniformTheHonestChain Nothing recipe' (testSeed :: QCGen)
            m     = H.checkHonestChain mutatedRecipe sched
        in
        case Exn.runExcept m of
            Right () -> go recipe' testSeedsSeed'
            Left e   -> case e of
                H.BadCount{}      -> error $ "impossible! " <> show e
                H.BadSgenWindow{} -> True
                H.BadLength{}     -> error $ "impossible! " <> show e
