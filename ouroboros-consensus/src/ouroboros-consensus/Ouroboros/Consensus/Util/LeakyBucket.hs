{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE NumericUnderscores  #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module implements a “leaky bucket”. One defines a bucket with a
-- capacity and a leaking rate; a race (in the sense of Async) starts against
-- the bucket which leaks at the given rate. The user is provided with a
-- function to refill the bucket by a certain amount. If the bucket ever goes
-- empty, both threads are cancelled.
--
-- This can be used for instance to enforce a minimal rate of a peer: they race
-- against the bucket and refill the bucket by a certain amount whenever they do
-- a “good” action.
--
-- NOTE: Even though the imagery is the same, this is different from what is
-- usually called a “token bucket” or “leaky bucket” in the litterature where it
-- is mostly used for rate limiting.
--
-- REVIEW: Could maybe be used for the dual leaky bucket used for rate limiting
-- algorithm by giving a negative rate, starting at 0 and not making reaching 0
-- a termination cause.
module Ouroboros.Consensus.Util.LeakyBucket (
    Config (..)
  , Handler (..)
  , Snapshot (..)
  , diffTimeToSecondsRational
  , evalAgainstBucket
  , execAgainstBucket
  , runAgainstBucket
  , withPause
  , withPauseIf
  ) where

import           Data.Ratio ((%))
import           Data.Time (DiffTime)
import           Data.Time.Clock (diffTimeToPicoseconds)
import           Ouroboros.Consensus.Util.IOLike
                     (ExceptionInLinkedThread (ExceptionInLinkedThread),
                     MonadAsync (async), MonadCatch (handle),
                     MonadDelay (threadDelay), MonadFork, MonadMask,
                     MonadMonotonicTime, MonadSTM, MonadThrow (throwIO),
                     StrictTVar, Time, atomically, diffTime, getMonotonicTime,
                     link, readTVar, uncheckedNewTVarM, writeTVar)
import           Prelude hiding (init)

-- | Configuration of a leaky bucket.
data Config m = Config {
  capacity       :: Rational,
  -- ^ Initial and maximal capacity of the bucket.
  rate           :: Rational,
  -- ^ Tokens per second leaking off the bucket.
  fillOnOverflow :: Bool,
  -- ^ Whether to fill to capacity on overflow or to do nothing.
  onEmpty        :: m ()
  -- ^ A monadic action to trigger when the bucket is empty.
  }

-- | Snapshot of a leaky bucket, giving the level and the associated time.
data Snapshot = Snapshot {
  level :: Rational,
  time  :: Time
  }
  deriving (Eq, Show)

-- | A bucket: a configuration and a state, which is just a TVar of snapshots.
data Bucket m = Bucket {
  config :: Config m,
  state  :: StrictTVar m Snapshot,
  paused :: StrictTVar m Bool
  }

-- | Whether filling the bucket overflew.
newtype Overflew = Overflew Bool

-- | The handler to a bucket: contains the API to interact with a running
-- bucket.
data Handler m = Handler {
  fill   :: Rational -> m Overflew,
  -- ^ Refill the bucket by the given amount and returns whether the bucket
  -- overflew. The bucket may silently get filled to full capacity or not get
  -- filled depending on 'fillOnOverflow'.
  pause  :: m (),
  -- ^ Pause the bucket, stopping it from leaking until it is resumed. It is
  -- still possible to fill it during that time. It does not matter if 'pause'
  -- is called again on a paused bucket.
  resume :: m ()
  -- ^ Resume the bucket, making it leak again. It does not matter if 'resume'
  -- is called on a running bucket.
  }

-- | Perform the given action with the bucket paused.
withPause :: Monad m => Handler m -> m a -> m a
withPause = withPauseIf $ pure True

-- | Perform the given action. The bucket is paused if the given boolean
-- evaluates to 'True'.
withPauseIf :: Monad m => m Bool -> Handler m -> m a -> m a
withPauseIf test handler action =
  test >>= \case
    True -> do
      pause handler
      result <- action
      resume handler
      pure result
    False -> action

-- | Create a bucket with the given configuration, then run the action against
-- that bucket. Returns when the action terminates or the bucket empties. In the
-- first case, return the value returned by the action. In the second case,
-- return @Nothing@.
execAgainstBucket ::
  (MonadDelay m, MonadAsync m, MonadFork m, MonadMask m) =>
  Config m ->
  (Handler m -> m a) ->
  m a
execAgainstBucket config action = snd <$> runAgainstBucket config action

-- | Same as 'execAgainstBucket' but also returns a 'Snapshot' of the bucket
-- when the action terminates.
runAgainstBucket ::
  (MonadDelay m, MonadAsync m, MonadFork m, MonadMask m) =>
  Config m ->
  (Handler m -> m a) ->
  m (Snapshot, a)
runAgainstBucket config action = do
    bucket <- init config
    leakThread <- async $ leak bucket
    handle rethrowUnwrap $ do
      link leakThread
      result <- action $ Handler {
        fill = (snd <$>) . takeSnapshotFill bucket,
        pause = takeSnapshot bucket >> atomically (writeTVar (paused bucket) True),
        resume = takeSnapshot bucket >> atomically (writeTVar (paused bucket) False)
        }
      snapshot <- takeSnapshot bucket
      pure (snapshot, result)
  where
    rethrowUnwrap :: MonadThrow m => ExceptionInLinkedThread -> m (Snapshot, a)
    rethrowUnwrap (ExceptionInLinkedThread _ e) = throwIO e

-- | Same as 'runAgainstBucket' but only returns a 'Snapshot' of the bucket when
-- the action terminates.
evalAgainstBucket ::
  (MonadDelay m, MonadAsync m, MonadFork m, MonadMask m) =>
  Config m ->
  (Handler m -> m a) ->
  m Snapshot
evalAgainstBucket config action = fst <$> runAgainstBucket config action

-- | Initialise a bucket given a configuration. The bucket starts full at the
-- time where one calls 'init'.
init :: (MonadMonotonicTime m, MonadSTM m) => Config m -> m (Bucket m)
init config@Config{capacity} = do
  time <- getMonotonicTime
  state <- uncheckedNewTVarM $ Snapshot{time, level = capacity}
  paused <- uncheckedNewTVarM False
  pure $ Bucket{config, state, paused}

-- | Monadic action that calls 'threadDelay' until the bucket is empty, then
-- returns @()@.
leak :: (MonadSTM m, MonadDelay m) => Bucket m -> m ()
leak bucket@Bucket{config=Config{rate, onEmpty}} = do
  Snapshot{level} <- takeSnapshot bucket
  let timeToWait = secondsRationalToDiffTime (level / rate)
  -- NOTE: It is possible that @timeToWait == 0@ while @level > 0@ when @level@
  -- is so tiny that @level / rate@ rounds down to 0 picoseconds. In that case,
  -- it is safe to assume that it is just zero.
  if level <= 0 || timeToWait == 0
    then onEmpty
    else threadDelay timeToWait >> leak bucket

-- | Take a snapshot of the bucket, that is compute its state at the current
-- time.
takeSnapshot :: (MonadSTM m, MonadMonotonicTime m) => Bucket m -> m Snapshot
takeSnapshot bucket = fst <$> takeSnapshotFill bucket 0

-- | Same as 'takeSnapshot' but also adds the given quantity to the resulting
-- level and returns whether this action overflew the bucket.
--
-- REVIEW: What to do when 'toAdd' is negative?
takeSnapshotFill :: (MonadSTM m, MonadMonotonicTime m) => Bucket m -> Rational -> m (Snapshot, Overflew)
takeSnapshotFill Bucket{config=Config{rate,capacity,fillOnOverflow}, state, paused} toAdd = do
  newTime <- getMonotonicTime
  atomically $ do
    Snapshot {level, time} <- readTVar state
    isPaused <- readTVar paused
    let elapsed = diffTime newTime time
        leaked = if isPaused then 0 else (diffTimeToSecondsRational elapsed * rate)
        levelLeaked = max 0 (level - leaked)
        levelFilled = min capacity (levelLeaked + toAdd)
        overflew = levelLeaked + toAdd > capacity
        newLevel = if not overflew || fillOnOverflow then levelFilled else levelLeaked
        snapshot = Snapshot {time = newTime, level = newLevel}
    writeTVar state snapshot
    pure (snapshot, Overflew overflew)

-- | Convert a 'DiffTime' to a 'Rational' number of seconds. This is similar to
-- 'diffTimeToSeconds' but with picoseconds precision.
diffTimeToSecondsRational :: DiffTime -> Rational
diffTimeToSecondsRational = (% 1_000_000_000_000) . diffTimeToPicoseconds

-- | Alias of 'realToFrac' to make code more readable and typing more explicit.
secondsRationalToDiffTime :: Rational -> DiffTime
secondsRationalToDiffTime = realToFrac
