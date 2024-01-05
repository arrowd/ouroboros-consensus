{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeOperators    #-}

module Ouroboros.Consensus.Storage.LedgerDB (
    -- * API
    module Ouroboros.Consensus.Storage.LedgerDB.API
  , module Ouroboros.Consensus.Storage.LedgerDB.API.Config
  , module Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
    -- * Impl
  , openDB
  ) where

import           Control.Monad.Base
import           Data.Functor.Identity
import           Data.Word
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.Inspect
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Storage.ImmutableDB (ImmutableDB)
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Init hiding (openDB)
import qualified Ouroboros.Consensus.Storage.LedgerDB.Init as Init
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.Init as V1
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.Init as V2
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike

openDB ::
  forall m l blk.
  ( IOLike m
  , MonadBase m m
  , LedgerSupportsProtocol blk
  , LedgerDbSerialiseConstraints blk
  , InspectLedger blk
  , HasCallStack
  , l ~ ExtLedgerState blk
  )
  => LedgerDBArgs Identity m blk
  -- ^ Stateless initializaton arguments
  -> ImmutableDB m blk
  -- ^ Reference to the immutable DB
  --
  -- After reading a snapshot from disk, the ledger DB will be brought up to
  -- date with tip of the immutable DB. The corresponding ledger state can then
  -- be used as the starting point for chain selection in the ChainDB driver.
  -> Point blk
  -- ^ The Replay goal i.e. the immutable tip.
  -> ResolveBlock m blk
  -- ^ How to get blocks from the ChainDB
  -> m (LedgerDB m l blk, Word64)
openDB
  args@LedgerDBArgs { lgrBackendSelector = V1InMemory }
  immdb
  replayGoal
  getBlock = do
    let initDb = V1.mkInitDb
                   args
                   V1.InMemoryBackingStore
                   getBlock
    Init.openDB args initDb immdb replayGoal
openDB
  args@LedgerDBArgs { lgrBackendSelector = V1LMDB limits }
  immdb
  replayGoal
  getBlock = do
    let initDb = V1.mkInitDb
                   args
                   (V1.LMDBBackingStore limits)
                   getBlock
    Init.openDB args initDb immdb replayGoal
openDB
  args@LedgerDBArgs { lgrBackendSelector = V2InMemory }
  immdb
  replayGoal
  getBlock = do
    let initDb = V2.mkInitDb
                   args
                   V2.InMemory
                   getBlock
    Init.openDB args initDb immdb replayGoal
openDB
  args@LedgerDBArgs { lgrBackendSelector = V2LSM }
  immdb
  replayGoal
  getBlock = do
    let initDb = V2.mkInitDb
                   args
                   V2.LSM
                   getBlock
    Init.openDB args initDb immdb replayGoal
