{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE PolyKinds                #-}
{-# LANGUAGE RankNTypes               #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}
-- |

module Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors (
    HasFlavorArgs (..)
  , LedgerDbFlavor (..)
  , LedgerDbStorageFlavor (..)
  , Sing (..)
  ) where

import           Data.Kind (Constraint, Type)
import           Ouroboros.Consensus.Util.Singletons

data LedgerDbStorageFlavor where
   InMemory :: LedgerDbStorageFlavor
   OnDisk   :: LedgerDbStorageFlavor

data instance Sing (f :: LedgerDbStorageFlavor) where
   SInMemory :: Sing InMemory
   SOnDisk   :: Sing OnDisk

instance SingI InMemory where
  sing = SInMemory

instance SingI OnDisk where
  sing = SOnDisk

data LedgerDbFlavor =
    FlavorV1
  | FlavorV2

data instance Sing (g :: LedgerDbFlavor) where
  SFlavorV1 :: Sing FlavorV1
  SFlavorV2 :: Sing FlavorV2

instance SingI FlavorV1 where
  sing = SFlavorV1

instance SingI FlavorV2 where
  sing = SFlavorV2

type HasFlavorArgs ::
     LedgerDbFlavor
  -> LedgerDbStorageFlavor
  -> (Type -> Type)
  -> Constraint
class (SingI f, SingI g) => HasFlavorArgs f g m where
  data family LedgerDbFlavorArgs f g m
  defaultFlavorArgs :: LedgerDbFlavorArgs f g m
