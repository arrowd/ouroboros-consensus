{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE PolyKinds                #-}
{-# LANGUAGE QuantifiedConstraints    #-}
{-# LANGUAGE RankNTypes               #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}

-- | The available flavors of the LedgerDB.

module Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors (
    Flavor
  , HasFlavorArgs (..)
  , Impl
  , LedgerDbFlavor (..)
  , LedgerDbImplementation
  , LedgerDbStorageFlavor (..)
  , Sing (..)
  ) where

import           Data.Kind (Constraint, Type)
import           Data.SOP.Dict
import           Ouroboros.Consensus.Util.Singletons

data LedgerDbStorageFlavor =
     InMemory
   | OnDisk

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

type HasFlavorArgs ::
     LedgerDbImplementation
  -> (Type -> Type)
  -> Constraint
class SingI impl => HasFlavorArgs impl m where
  data family LedgerDbFlavorArgs impl m
  defaultFlavorArgs :: LedgerDbFlavorArgs impl m

type Flavor :: (LedgerDbFlavor, LedgerDbStorageFlavor) -> LedgerDbFlavor
type family Flavor x where
  Flavor '(a, b) = a

type Impl :: (LedgerDbFlavor, LedgerDbStorageFlavor) -> LedgerDbStorageFlavor
type family Impl x where
  Impl '(a, b) = b

type LedgerDbImplementation = (LedgerDbFlavor, LedgerDbStorageFlavor)

data instance Sing (g :: LedgerDbImplementation) where
  SFlavorV1' :: Dict SingI impl -> Sing '(FlavorV1, impl)
  SFlavorV2' :: Dict SingI impl -> Sing '(FlavorV2, impl)

instance SingI impl => SingI '(FlavorV1, impl :: LedgerDbStorageFlavor) where
  sing = SFlavorV1' Dict

instance SingI impl => SingI '(FlavorV2, impl :: LedgerDbStorageFlavor) where
  sing = SFlavorV2' Dict
