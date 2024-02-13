{-# LANGUAGE PatternSynonyms #-}

module Test.Ouroboros.Consensus.ChainGenerator.Params (
    Asc (Asc, UnsafeAsc)
  , Delta (Delta)
  , Kcp (Kcp)
  , Len (Len)
  , Sfor (Sfor)
  , Sgen (Sgen)
  , ascFromBits
  , ascFromDouble
  , ascVal
  , genAsc
  , genKSSD
  ) where

import qualified Data.Bits as B
import           Data.Word (Word8)
import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Extras (sized1)

-----

-- | The Δ parameter of the Praos theorems and so also of the Praos Race
-- Assumption
--
-- ASSUMPTION: If an honest block @b@ is minted at the start of slot @x@, then
-- every (healthy) honest node will have selected a chain no worse than @b@ by
-- the onset of slot @x + Δ + 1@.
--
-- NOTE: If @Δ=0@, then the best block minted in each slot is selected by every
-- (healthy) honest node before the onset of the next slot.
--
-- NOTE: If the honest block @k+1@ after its intersection with an alternative
-- chain was minted in slot @x@, then the alternative block @k+1@ after the
-- intersection can be minted no sooner than slot @x + Δ + 1@. Thus @x + Δ@ is
-- the youngest slot in the Praos Race Window.
newtype Delta = Delta Int
  deriving (Eq, Ord, Show, Read)

-- | The maximum length of any leader schedule
--
-- This can be interpreted as the /end of time/, the final moment simulated
-- during a test.
newtype Len = Len Int
  deriving (Eq, Ord, Show, Read)

-- | The @k@ parameter of the Praos Common Prefix property
--
-- Also known as the 'Ouroboros.Consensus.Config.SecurityParam.SecurityParam'.
newtype Kcp = Kcp Int
  deriving (Eq, Ord, Show, Read)

-- | The @s@ parameters.
--
-- There are three of them, all describing a number of slots:
--
-- - @scg@, the @s@ parameter of the Praos Chain Growth property,
-- - @sgen@, the size of the Genesis window, and
-- - @sfor@, the size of the forecast window.
--
-- @scg@ is a theoretical parameter. It is also known as the width of the
-- /stability window/, in which an adversarial stake holder cannot drastically
-- increase their rate of election until at least @scg@ many slots after the
-- first block on an adversarial chain. In other words: we're assuming that any
-- serious attempt to corrupt the leader schedule would be isolated to a private
-- adversarial chain.
--
-- @sgen@ and @sfor@ are concrete parameters of the chain that affect the
-- behaviour of a node. @sgen@ determines the amount of slots which we will
-- consider to decide of a better chain in the Genesis algorithm. @sfor@
-- determines how many slots in advance we will allow ourselves to validate
-- headers.
--
-- We must have @sgen <= scg@ because it would otherwise not be guaranteed that
-- the adversary cannot make itself extra-dense in the Genesis window. We must
-- have @sgen <= sfor@ because it would otherwise not always be possible to
-- validate all the headers in the Genesis window. In practice, we will take
-- @sgen = sfor = scg@.

newtype Sgen = Sgen Int
  deriving (Eq, Ord, Show, Read)

newtype Sfor = Sfor Int
  deriving (Eq, Ord, Show, Read)

-----

-- | The /active slot coefficient/
--
-- INVARIANT: 0 < x < 1
--
-- It's as precise as 'Double', which likely suffices for all of our needs.
newtype Asc = UnsafeAsc Double
  deriving (Eq, Read, Show)

pattern Asc :: Double -> Asc
pattern Asc d <- UnsafeAsc d

{-# COMPLETE Asc #-}

ascFromDouble :: Double -> Asc
ascFromDouble d
  | d <= 0    = error "Asc must be > 0"
  | 1 <= d    = error "Asc must be < 1"
  | otherwise = UnsafeAsc d

-- | PRECONDITION: the bits aren't all the same
--
-- The 'Asc' that equals the fraction @w \/ 2^widthW@.
ascFromBits :: (Enum w, B.FiniteBits w) => w -> Asc
ascFromBits w = ascFromDouble $ toEnum (fromEnum w) / (2 ^ B.finiteBitSize w)

-- | Interpret 'Asc' as a 'Double'
ascVal :: Asc -> Double
ascVal (Asc x) = x

genAsc :: QC.Gen Asc
genAsc = ascFromBits <$> QC.choose (1 :: Word8, maxBound - 1)

genKSSD :: QC.Gen (Kcp, Sgen, Sfor, Delta)
genKSSD = sized1 $ \sz -> do
    -- k > 1 so we can ensure an alternative schema loses the density comparison
    -- without having to deactivate the first active slot
    k <- (+ 2) <$> QC.choose (0, sz)
    sgen <- (+ k) <$> QC.choose (0, 2 * sz)   -- ensures @k / s <= 1@
    let sfor = sgen
    d <- QC.choose (0, max 0 $ min (div sz 4) (sgen-1)) -- ensures @d < s@
    pure (Kcp k, Sgen sgen, Sfor sfor, Delta d)
