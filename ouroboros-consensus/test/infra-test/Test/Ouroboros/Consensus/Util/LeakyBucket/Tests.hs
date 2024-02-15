{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE NumericUnderscores  #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Ouroboros.Consensus.Util.LeakyBucket.Tests (tests) where

import           Control.Monad (foldM, void)
import           Control.Monad.IOSim (IOSim, runSimOrThrow)
import           Data.Either (isLeft, isRight)
import           Data.Functor ((<&>))
import           Data.Ratio ((%))
import           Data.Time.Clock (DiffTime, picosecondsToDiffTime)
import           Ouroboros.Consensus.Util.IOLike (Exception (displayException),
                     MonadCatch (try), MonadDelay, MonadThrow (throwIO),
                     SomeException, Time (Time), addTime, fromException,
                     threadDelay)
import           Ouroboros.Consensus.Util.LeakyBucket
import           Test.QuickCheck (Arbitrary (arbitrary), Gen, Property,
                     classify, counterexample, forAll, frequency, ioProperty,
                     listOf1, scale, suchThat, (===))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (property, testProperty)
import           Test.Util.TestEnv (adjustQuickCheckTests)

tests :: TestTree
tests = testGroup "Ouroboros.Consensus.Util.LeakyBucket" [
  testProperty "play a bit" prop_playABit,
  testProperty "play too long" prop_playTooLong,
  testProperty "play too long harmless" prop_playTooLongHarmless,
  testProperty "wait almost too long" (prop_noRefill (-1)),
  testProperty "wait just too long" (prop_noRefill 1),
  testProperty "pause for a time" prop_playWithPause,
  testProperty "resume too quickly" prop_playWithPauseTooLong,
  testProperty "propagates exceptions" prop_propagateExceptions,
  testProperty "propagates exceptions (IO)" prop_propagateExceptionsIO,
  testProperty "catch exception" prop_catchException,
  adjustQuickCheckTests (* 10) $ testProperty "random" prop_random
  ]

--------------------------------------------------------------------------------
-- Dummy configuration
--------------------------------------------------------------------------------

newtype Capacity = Capacity Rational
  deriving Show

instance Arbitrary Capacity where
  arbitrary = Capacity <$> arbitrary `suchThat` (> 0)

newtype Rate = Rate Rational
  deriving Show

instance Arbitrary Rate where
  arbitrary = Rate <$> arbitrary `suchThat` (> 0)

newtype FillOnOverflow = FillOnOverflow Bool
  deriving Show

instance Arbitrary FillOnOverflow where
  arbitrary = FillOnOverflow <$> arbitrary

-- | Make a configuration from a 'Capacity', a 'Rate' and an 'onEmpty' action.
config :: Capacity -> Rate -> FillOnOverflow -> m () -> Config m
config (Capacity capacity) (Rate rate) (FillOnOverflow fillOnOverflow) onEmpty =
  Config{capacity, rate, fillOnOverflow, onEmpty}

data EmptyBucket = EmptyBucket
  deriving (Eq, Show)

instance Exception EmptyBucket

-- | Make a configuration that fills on overflow and throws 'EmptyBucket' on
-- empty bucket.
configFillThrow :: MonadThrow m => Capacity -> Rate -> Config m
configFillThrow c r = config c r (FillOnOverflow True) (throwIO EmptyBucket)

-- | A configuration with capacity and rate 1, that fills on overflow and throws
-- 'EmptyBucket' on empty bucket.
config11FillThrow :: MonadThrow m => Config m
config11FillThrow = configFillThrow (Capacity 1) (Rate 1)

-- | Make a configuration that fills on overflow and does nothing on empty
-- bucket.
configFillPure :: Applicative m => Capacity -> Rate -> Config m
configFillPure c r = config c r (FillOnOverflow True) (pure ())

-- | A configuration with capacity 1 and rate 1, that fills on overflow and does
-- nothing on empty bucket.
config11FillPure :: Applicative m => Config m
config11FillPure = configFillPure (Capacity 1) (Rate 1)

-- | Whether to throw on empty bucket.
newtype ThrowOnEmpty = ThrowOnEmpty Bool
  deriving (Eq, Show)

instance Arbitrary ThrowOnEmpty where
  arbitrary = ThrowOnEmpty <$> arbitrary

-- | Make a configuration that fills on overflow from whether to 'ThrowOnEmpty'.
configFill' :: MonadThrow m => Capacity -> Rate -> ThrowOnEmpty -> Config m
configFill' c r (ThrowOnEmpty True)  = configFillThrow c r
configFill' c r (ThrowOnEmpty False) = configFillPure c r

-- | Alias for 'runSimOrThrow' by analogy to 'ioProperty'.
ioSimProperty :: forall a. (forall s. IOSim s a) -> a
ioSimProperty = runSimOrThrow

-- | QuickCheck helper to check that a code threw the given exception.
shouldThrow :: (MonadCatch m, Show a, Exception e, Eq e) => m a -> e -> m Property
shouldThrow a e =
  try a <&> \case
    Left exn
      | fromException exn == Just e -> property True
      | otherwise -> counterexample ("Expected exception " ++ show e ++ "; got exception " ++ show exn) False
    Right result -> counterexample ("Expected exception " ++ show e ++ "; got " ++ show result) False

-- | QuickCheck helper to check that a code evaluated to the given value.
shouldEvaluateTo :: (MonadCatch m, Eq a, Show a) => m a -> a -> m Property
shouldEvaluateTo a v =
  try a <&> \case
    Right result
      | result == v -> property True
      | otherwise -> counterexample ("Expected " ++ show v ++ "; got " ++ show result) False
    Left (exn :: SomeException) -> counterexample ("Expected " ++ show v ++ "; got exception " ++ displayException exn) False

-- | Number of picoseconds in a second (@10^12@).
picosecondsPerSecond :: Integer
picosecondsPerSecond = 1_000_000_000_000

--------------------------------------------------------------------------------
-- Simple properties
--------------------------------------------------------------------------------

-- | One test case where we wait a bit, then fill, then wait some more. We then
-- should observe a snapshot with a positive level.
prop_playABit :: Property
prop_playABit =
  ioSimProperty $
    evalAgainstBucket config11FillThrow (\handler -> do
      threadDelay 0.5
      void $ fill handler 67
      threadDelay 0.9
    ) `shouldEvaluateTo` Snapshot{level = 1 % 10, time = Time 1.4}

-- | One test case similar to 'prop_playABit' but we wait a bit too long and
-- should observe the triggering of the 'onEmpty' action.
prop_playTooLong :: Property
prop_playTooLong =
  ioSimProperty $
    evalAgainstBucket config11FillThrow (\handler -> do
      threadDelay 0.5
      void $ fill handler 67
      threadDelay 1.1
    ) `shouldThrow` EmptyBucket

-- | One test case similar to 'prop_playTooLong' but 'onEmpty' does nothing and
-- therefore we should still observe a snapshot at the end.
prop_playTooLongHarmless :: Property
prop_playTooLongHarmless =
  ioSimProperty $
    evalAgainstBucket config11FillPure (\handler -> do
      threadDelay 0.5
      void $ fill handler 67
      threadDelay 1.1
    ) `shouldEvaluateTo` Snapshot{level = 0, time = Time 1.6}

prop_playWithPause :: Property
prop_playWithPause =
  ioSimProperty $
    evalAgainstBucket config11FillThrow (\handler -> do
      threadDelay 0.5
      pause handler
      threadDelay 1.5
      resume handler
      threadDelay 0.4
    ) `shouldEvaluateTo` Snapshot{level = 1 % 10, time = Time 2.4}

prop_playWithPauseTooLong :: Property
prop_playWithPauseTooLong =
  ioSimProperty $
    evalAgainstBucket config11FillThrow (\handler -> do
      threadDelay 0.5
      pause handler
      threadDelay 1.5
      resume handler
      threadDelay 0.6
    ) `shouldThrow` EmptyBucket

-- | A bunch of test cases where we wait exactly as much as the bucket runs
-- except for a given offset. If the offset is negative, we should get a
-- snapshot. If the offset is positive, we should get an exception. NOTE: Do not
-- use an offset of @0@. NOTE: Considering the precision, we *need* IOSim for
-- this test.
prop_noRefill :: Integer -> Capacity -> Rate -> Property
prop_noRefill offset capacity@(Capacity c) rate@(Rate r) = do
  -- NOTE: The @-1@ is to ensure that we do not test the situation where the
  -- bucket empties at the *exact* same time (curtesy of IOSim) as the action.
  let ps = floor (c / r * fromInteger picosecondsPerSecond) + offset
      time = picosecondsToDiffTime ps
      level = c - (ps % picosecondsPerSecond) * r
  if
    | offset < 0 ->
      ioSimProperty $
        evalAgainstBucket (configFillThrow capacity rate) (\_ -> threadDelay time)
        `shouldEvaluateTo` Snapshot{level, time = Time time}
    | offset > 0 ->
      ioSimProperty $
        evalAgainstBucket (configFillThrow capacity rate) (\_ -> threadDelay time)
        `shouldThrow` EmptyBucket
    | otherwise ->
      error "prop_noRefill: do not use an offset of 0"

--------------------------------------------------------------------------------
-- Exception propagation
--------------------------------------------------------------------------------

-- | A dummy exception that we will use to outrun the bucket.
data NoPlumberException = NoPlumberException
  deriving (Eq, Show)
instance Exception NoPlumberException

-- | One test to check that throwing an exception in the action does propagate
-- outside of @*AgainstBucket@.
prop_propagateExceptions :: Property
prop_propagateExceptions =
  ioSimProperty $
    evalAgainstBucket config11FillThrow (\_ -> throwIO NoPlumberException)
      `shouldThrow`
    NoPlumberException

-- | Same as 'prop_propagateExceptions' except it runs in IO.
prop_propagateExceptionsIO :: Property
prop_propagateExceptionsIO =
  ioProperty $
    evalAgainstBucket config11FillThrow (\_ -> throwIO NoPlumberException)
      `shouldThrow`
    NoPlumberException

-- | One test to show that we can catch the 'EmptyBucket' exception from the
-- action itself, but that it is not wrapped in 'ExceptionInLinkedThread'.
prop_catchException :: Property
prop_catchException =
  ioSimProperty $
    execAgainstBucket config11FillThrow (\_ -> try $ threadDelay 1000)
      `shouldEvaluateTo`
    Left EmptyBucket

--------------------------------------------------------------------------------
-- Against a model
--------------------------------------------------------------------------------

-- | Abstract “actions” to be run. We can either wait by some time or refill the
-- bucket by some value.
data Action = ThreadDelay DiffTime | Fill Rational | Pause | Resume
  deriving (Eq, Show)

-- | Random generation of 'Action's. The scales and frequencies are taken such
-- that we explore as many interesting cases as possible.
genAction :: Gen Action
genAction = frequency [
  (1, ThreadDelay . picosecondsToDiffTime <$> scale (* fromInteger picosecondsPerSecond) (arbitrary `suchThat` (>= 0))),
  (1, Fill <$> scale (* 1_000_000_000_000_000) (arbitrary `suchThat` (>= 0))),
  (1, pure Pause),
  (1, pure Resume)
  ]

-- | How to run the 'Action's in a monad.
applyActions :: MonadDelay m => Handler m -> [Action] -> m ()
applyActions handler = mapM_ $ \case
  ThreadDelay t -> threadDelay t
  Fill t -> void $ fill handler t
  Pause -> pause handler
  Resume -> resume handler

data BucketModel = BucketModel {
  snapshot :: Snapshot,
  paused   :: Bool
  }

-- | A model of what we expect the 'Action's to lead to, either an 'EmptyBucket'
-- exception (if the bucket won the race) or a 'Snapshot' (otherwise).
modelActions :: Capacity -> Rate -> ThrowOnEmpty -> [Action] -> Either EmptyBucket BucketModel
modelActions (Capacity capacity) (Rate rate) (ThrowOnEmpty throwOnEmpty) =
    foldM go $ BucketModel{snapshot=Snapshot{level=capacity, time=Time 0}, paused=False}
  where
    go bucketModel@BucketModel{snapshot=Snapshot{time, level}} = \case
      Fill t ->
        Right bucketModel{snapshot=Snapshot{time, level = min capacity (level + t)}}
      ThreadDelay t ->
        let newTime = addTime t time
            newLevel = if paused bucketModel then level else max 0 (level - diffTimeToSecondsRational t * rate)
         in if newLevel <= 0 && throwOnEmpty
              then Left EmptyBucket
              else Right bucketModel{snapshot=Snapshot{time = newTime, level = newLevel}}
      Pause ->
        Right bucketModel{paused=True}
      Resume ->
        Right bucketModel{paused=False}

-- | A bunch of test cases where we generate a list of 'Action's ,run them via
-- 'applyActions' and compare the result to that of 'modelActions'.
prop_random :: Capacity -> Rate -> ThrowOnEmpty -> Property
prop_random capacity rate throwOnEmpty =
  forAll (listOf1 genAction) $ \actions ->
    let modelResult = snapshot <$> modelActions capacity rate throwOnEmpty actions
        nbActions = length actions
     in classify (isLeft modelResult) "bucket finished empty" $
        classify (isRight modelResult) "bucket finished non-empty" $
        classify (nbActions <= 10) "<= 10 actions" $
        classify (10 < nbActions && nbActions <= 20) "11-20 actions" $
        classify (20 < nbActions && nbActions <= 50) "21-50 actions" $
        classify (50 < nbActions) "> 50 actions" $
        runSimOrThrow (
          try $ evalAgainstBucket (configFill' capacity rate throwOnEmpty) $
            flip applyActions actions
        ) === modelResult
