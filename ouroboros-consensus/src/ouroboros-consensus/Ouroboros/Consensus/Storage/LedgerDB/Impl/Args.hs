{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE NamedFieldPuns       #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
-- |

module Ouroboros.Consensus.Storage.LedgerDB.Impl.Args (
    LedgerDbArgs (..)
  , SomeLedgerDbArgs (..)
  , defaultArgs
  ) where

import           Control.Tracer
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.API.Config
import           Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Util.Args
import           Ouroboros.Consensus.Util.Singletons
import           System.FS.API

{-------------------------------------------------------------------------------
  Arguments
-------------------------------------------------------------------------------}

-- | Arguments required to initialize a LedgerDB.
data LedgerDbArgs f flavor impl m blk = (SingI flavor, SingI impl) => LedgerDbArgs {
      lgrSnapshotPolicy :: HKD f SnapshotPolicy
    , lgrGenesis        :: HKD f (m (ExtLedgerState blk ValuesMK))
    , lgrHasFS          :: SomeHasFS m
    , lgrConfig         :: HKD f (LedgerDbCfg (ExtLedgerState blk))
    , lgrTracer         :: Tracer m (TraceLedgerDBEvent blk)
    , lgrQueryBatchSize :: QueryBatchSize
    , lgrFlavorArgs     :: LedgerDbFlavorArgs flavor impl m
    }

data SomeLedgerDbArgs f m blk where
  SomeLedgerDbArgs ::
       LedgerDbArgs f flavor impl m blk
    -> SomeLedgerDbArgs f m blk

-- | Default arguments
defaultArgs ::
     ( HasFlavorArgs flavor impl m
     , Applicative m
     )
  => SomeHasFS m
  -> Incomplete LedgerDbArgs flavor impl m blk
defaultArgs lgrHasFS = LedgerDbArgs {
      lgrSnapshotPolicy = NoDefault
    , lgrGenesis        = NoDefault
    , lgrHasFS
    , lgrConfig         = NoDefault
    , lgrTracer         = nullTracer
    , lgrQueryBatchSize = DefaultQueryBatchSize
    , lgrFlavorArgs     = defaultFlavorArgs
    }
