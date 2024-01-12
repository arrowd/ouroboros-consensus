{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Ouroboros.Consensus.Storage.LedgerDB.V2.Args () where

import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors

instance HasFlavorArgs FlavorV2 InMemory m where
  data instance LedgerDbFlavorArgs FlavorV2 InMemory m = V2InMemoryArgs
  defaultFlavorArgs = V2InMemoryArgs

instance HasFlavorArgs FlavorV2 OnDisk m where
  data instance LedgerDbFlavorArgs FlavorV2 OnDisk m = V2LSMArgs
  defaultFlavorArgs = V2LSMArgs
