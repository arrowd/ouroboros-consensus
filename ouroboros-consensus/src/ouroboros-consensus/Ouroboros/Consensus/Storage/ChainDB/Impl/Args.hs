{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneKindSignatures  #-}

module Ouroboros.Consensus.Storage.ChainDB.Impl.Args (
    ChainDbArgs (..)
  , ChainDbSpecificArgs (..)
  , RelativeMountPoint (..)
  , SomeChainDbArgs (..)
  , defaultArgs
    -- * Internal
  , cdbBlocksToAddSize
  , cdbCheckInFuture
  , cdbCheckIntegrity
  , cdbChunkInfo
  , cdbGcDelay
  , cdbGcInterval
  , cdbGenesis
  , cdbHasFSImmutableDB
  , cdbHasFSLedgerDB
  , cdbHasFSVolatileDB
  , cdbImmutableDbCacheConfig
  , cdbImmutableDbValidation
  , cdbMaxBlocksPerFile
  , cdbRegistry
  , cdbSnapshotPolicy
  , cdbTopLevelConfig
  , cdbTracer
  , cdbVolatileDbValidation
  ) where

import           Control.Tracer (Tracer, nullTracer)
import           Data.Kind
import           Data.Time.Clock (DiffTime, secondsToDiffTime)
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Fragment.InFuture (CheckInFuture)
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.Tables
import           Ouroboros.Consensus.Storage.ChainDB.Impl.Types
                     (TraceEvent (..))
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import qualified Ouroboros.Consensus.Storage.LedgerDB as LedgerDB
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Args as LedgerDB
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors as LedgerDB
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots as LedgerDB
import qualified Ouroboros.Consensus.Storage.VolatileDB as VolatileDB
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.ResourceRegistry (ResourceRegistry)
import           Ouroboros.Consensus.Util.Singletons
import           System.FS.API

{-------------------------------------------------------------------------------
  Arguments
-------------------------------------------------------------------------------}

data ChainDbArgs f flavor impl m blk = ChainDbArgs {
    cdbImmDbArgs :: ImmutableDB.ImmutableDbArgs f m blk
  , cdbVolDbArgs :: VolatileDB.VolatileDbArgs f m blk
  , cdbLgrDbArgs :: LedgerDB.LedgerDbArgs f flavor impl m blk
  , cdbsArgs     :: ChainDbSpecificArgs f flavor impl m blk
  }

cdbHasFSImmutableDB ::
     ChainDbArgs f flavor impl m blk
  -> SomeHasFS m
cdbHasFSImmutableDB = ImmutableDB.immHasFS . cdbImmDbArgs

cdbImmutableDbValidation ::
     ChainDbArgs f flavor impl m blk
  -> ImmutableDB.ValidationPolicy
cdbImmutableDbValidation = ImmutableDB.immValidationPolicy . cdbImmDbArgs

cdbChunkInfo ::
     ChainDbArgs f flavor impl m blk
  -> HKD f ImmutableDB.ChunkInfo
cdbChunkInfo = ImmutableDB.immChunkInfo . cdbImmDbArgs

cdbImmutableDbCacheConfig ::
     ChainDbArgs f flavor impl m blk
  -> ImmutableDB.CacheConfig
cdbImmutableDbCacheConfig = ImmutableDB.immCacheConfig . cdbImmDbArgs

cdbCheckIntegrity ::
     ChainDbArgs f flavor impl m blk
  -> HKD f (blk -> Bool)
cdbCheckIntegrity = ImmutableDB.immCheckIntegrity . cdbImmDbArgs

cdbHasFSVolatileDB ::
     ChainDbArgs f flavor impl m blk
  -> SomeHasFS m
cdbHasFSVolatileDB = VolatileDB.volHasFS . cdbVolDbArgs

cdbVolatileDbValidation ::
     ChainDbArgs f flavor impl m blk
  -> VolatileDB.BlockValidationPolicy
cdbVolatileDbValidation = VolatileDB.volValidationPolicy . cdbVolDbArgs

cdbMaxBlocksPerFile ::
     ChainDbArgs f flavor impl m blk
  -> VolatileDB.BlocksPerFile
cdbMaxBlocksPerFile = VolatileDB.volMaxBlocksPerFile . cdbVolDbArgs

cdbHasFSLedgerDB ::
     ChainDbArgs f flavor impl m blk
  -> SomeHasFS m
cdbHasFSLedgerDB = LedgerDB.lgrHasFS . cdbLgrDbArgs

cdbSnapshotPolicy ::
     ChainDbArgs f flavor impl m blk
  -> HKD f LedgerDB.SnapshotPolicy
cdbSnapshotPolicy = LedgerDB.lgrSnapshotPolicy . cdbLgrDbArgs

cdbGenesis ::
     ChainDbArgs f flavor impl m blk
  -> HKD f (m (ExtLedgerState blk ValuesMK))
cdbGenesis = LedgerDB.lgrGenesis . cdbLgrDbArgs

cdbCheckInFuture ::
     ChainDbArgs f flavor impl m blk
  -> HKD f (CheckInFuture m blk)
cdbCheckInFuture = cdbsCheckInFuture . cdbsArgs

cdbRegistry ::
     ChainDbArgs f flavor impl m blk
  -> HKD f (ResourceRegistry m)
cdbRegistry = cdbsRegistry . cdbsArgs

cdbGcDelay ::
     ChainDbArgs f flavor impl m blk
  -> DiffTime
cdbGcDelay = cdbsGcDelay . cdbsArgs

cdbGcInterval ::
     ChainDbArgs f flavor impl m blk
  -> DiffTime
cdbGcInterval = cdbsGcInterval . cdbsArgs

cdbBlocksToAddSize ::
     ChainDbArgs f flavor impl m blk
  -> Word
cdbBlocksToAddSize = cdbsBlocksToAddSize . cdbsArgs

cdbTopLevelConfig ::
     ChainDbArgs f flavor impl m blk
  -> HKD f (TopLevelConfig blk)
cdbTopLevelConfig = cdbsTopLevelConfig . cdbsArgs

cdbTracer ::
     ChainDbArgs f flavor impl m blk
  -> Tracer m (TraceEvent flavor impl blk)
cdbTracer = cdbsTracer . cdbsArgs

data SomeChainDbArgs f m blk where
  SomeChainDbArgs ::
       (SingI flavor, SingI impl)
    => ChainDbArgs f flavor impl m blk
    -> SomeChainDbArgs f m blk

-- | Arguments specific to the ChainDB, not to the ImmutableDB, VolatileDB, or
-- LedgerDB.
type ChainDbSpecificArgs :: (Type -> Type)  -> LedgerDB.LedgerDbFlavor -> LedgerDB.LedgerDbStorageFlavor -> (Type -> Type) -> Type -> Type
data ChainDbSpecificArgs f flavor impl m blk = ChainDbSpecificArgs {
      cdbsBlocksToAddSize :: Word
    , cdbsCheckInFuture   :: HKD f (CheckInFuture m blk)
    , cdbsGcDelay         :: DiffTime
      -- ^ Delay between copying a block to the ImmutableDB and triggering a
      -- garbage collection for the corresponding slot on the VolatileDB.
      --
      -- The goal of the delay is to ensure that the write to the ImmutableDB
      -- has been flushed to disk before deleting the block from the
      -- VolatileDB, so that a crash won't result in the loss of the block.
    , cdbsGcInterval      :: DiffTime
      -- ^ Batch all scheduled GCs so that at most one GC happens every
      -- 'cdbsGcInterval'.
    , cdbsRegistry        :: HKD f (ResourceRegistry m)
    , cdbsTracer          :: Tracer m (TraceEvent flavor impl blk)
    , cdbsTopLevelConfig  :: HKD f (TopLevelConfig blk)
    }

-- | Default arguments
--
-- The following fields must still be defined:
--
-- * 'cdbsTracer'
-- * 'cdbsRegistry'
-- * 'cdbsCheckInFuture'
--
-- We a 'cdbsGcDelay' of 60 seconds and a 'cdbsGcInterval' of 10 seconds, this
-- means (see the properties in "Test.Ouroboros.Storage.ChainDB.GcSchedule"):
--
-- * The length of the 'GcSchedule' queue is @<= ⌈gcDelay / gcInterval⌉ + 1@,
--   i.e., @<= 7@.
-- * The overlap (number of blocks in both the VolatileDB and the ImmutableDB)
--   is the number of blocks synced in @gcDelay + gcInterval@ = 70s. E.g, when
--   bulk syncing at 1k-2k blocks/s, this means 70k-140k blocks. During normal
--   operation, we receive 1 block/20s (for Byron /and/ for Shelley), meaning
--   at most 4 blocks.
-- * The unnecessary overlap (the blocks that we haven't GC'ed yet but could
--   have, because of batching) < the number of blocks sync in @gcInterval@.
--   E.g., when syncing at 1k-2k blocks/s, this means 10k-20k blocks. During
--   normal operation, we receive 1 block/20s, meaning at most 1 block.
defaultSpecificArgs :: Monad m => Incomplete ChainDbSpecificArgs flavor impl m blk
defaultSpecificArgs = ChainDbSpecificArgs {
      cdbsBlocksToAddSize = 10
    , cdbsCheckInFuture   = NoDefault
    , cdbsGcDelay         = secondsToDiffTime 60
    , cdbsGcInterval      = secondsToDiffTime 10
    , cdbsRegistry        = NoDefault
    , cdbsTracer          = nullTracer
    , cdbsTopLevelConfig  = NoDefault
    }

-- | Default arguments
--
-- See 'ImmutableDB.defaultArgs', 'VolatileDB.defaultArgs', 'LgrDB.defaultArgs',
-- and 'defaultSpecificArgs' for a list of which fields are not given a default
-- and must therefore be set explicitly.
defaultArgs ::
     forall m blk flavor impl.
     (Monad m, LedgerDB.HasFlavorArgs flavor impl m)
  => (RelativeMountPoint -> SomeHasFS m)
  -> Incomplete ChainDbArgs flavor impl m blk
defaultArgs mkFS =
   ChainDbArgs (ImmutableDB.defaultArgs immFS)
               (VolatileDB.defaultArgs  volFS)
               (LedgerDB.defaultArgs    lgrFS)
               defaultSpecificArgs
  where
    immFS, volFS, lgrFS :: SomeHasFS m

    immFS = mkFS $ RelativeMountPoint "immutable"
    volFS = mkFS $ RelativeMountPoint "volatile"
    lgrFS = mkFS $ RelativeMountPoint "ledger"

{-------------------------------------------------------------------------------
  Relative mount points
-------------------------------------------------------------------------------}

-- | A relative path for a 'MountPoint'
--
-- The root is determined by context.
newtype RelativeMountPoint = RelativeMountPoint FilePath
