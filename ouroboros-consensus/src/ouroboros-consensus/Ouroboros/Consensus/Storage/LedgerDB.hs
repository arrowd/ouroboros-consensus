{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Ouroboros.Consensus.Storage.LedgerDB (
    -- * API
    module Ouroboros.Consensus.Storage.LedgerDB.API
  , module Ouroboros.Consensus.Storage.LedgerDB.API.Config
  , module Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
    -- * Impl
  , LedgerDbFlavor (..)
  , LedgerDbStorageFlavor (..)
  , openDB
  ) where

import           Control.Monad.Base
import           Data.Word
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Ledger.Inspect
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Storage.ImmutableDB.Impl.Stream
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Init hiding (openDB)
import qualified Ouroboros.Consensus.Storage.LedgerDB.Impl.Init as Init
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.Init as V1
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.Init as V2
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.Singletons

openDB ::
  forall m blk.
  ( IOLike m
  , MonadBase m m
  , LedgerSupportsProtocol blk
  , LedgerDbSerialiseConstraints blk
  , InspectLedger blk
  , HasCallStack
  )
  => Complete SomeLedgerDbArgs m blk
  -- ^ Stateless initializaton arguments
  -> StreamAPI m blk blk
  -- ^ Reference to the immutable DB
  --
  -- After reading a snapshot from disk, the ledger DB will be brought up to
  -- date with tip of the immutable DB. The corresponding ledger state can then
  -- be used as the starting point for chain selection in the ChainDB driver.
  -> Point blk
  -- ^ The Replay goal i.e. the immutable tip.
  -> ResolveBlock m blk
  -- ^ How to get blocks from the ChainDB
  -> m (LedgerDB' m blk, Word64)
openDB
  (SomeLedgerDbArgs (args@LedgerDbArgs{} :: Complete LedgerDbArgs flavor impl m blk))
  immdb
  replayGoal
  getBlock =
    case sing :: Sing flavor of
      SFlavorV1 ->
        let initDb = V1.mkInitDb
                       args
                       getBlock
        in
          Init.openDB args initDb immdb replayGoal
      SFlavorV2 ->
        let initDb = V2.mkInitDb
                       args
                       getBlock
        in
          Init.openDB args initDb immdb replayGoal
