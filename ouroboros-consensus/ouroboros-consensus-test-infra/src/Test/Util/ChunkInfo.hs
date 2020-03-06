module Test.Util.ChunkInfo (
    SmallChunkInfo(..)
  ) where

import           Test.QuickCheck

import           Ouroboros.Consensus.Storage.ImmutableDB.Chunks

{-------------------------------------------------------------------------------
  ChunkInfo

  This is defined here rather than in @test-infra@ as this is (somewhat)
  internal to the immutable DB.
-------------------------------------------------------------------------------}

data SmallChunkInfo = SmallChunkInfo ChunkInfo
  deriving (Show)

instance Arbitrary SmallChunkInfo where
  -- TODO: Generalize
  arbitrary = return $ SmallChunkInfo $ simpleChunkInfo fixedEpochSize
    where
      fixedEpochSize = 10

  -- Intentionally no shrinker, as shrinking the epoch size independent from
  -- the rest of the commands may lead to a non-sensical test
