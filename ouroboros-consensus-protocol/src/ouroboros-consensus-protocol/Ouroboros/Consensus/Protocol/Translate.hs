{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Ouroboros.Consensus.Protocol.Translate (TranslateProto (..)) where

import           Ouroboros.Consensus.Protocol.Abstract

-- | Translate across protocols
class TranslateProto protoFrom protoTo
  where
  translateConsensusConfig ::
    ConsensusConfig protoFrom -> ConsensusConfig protoTo
  -- | Translate the ledger view.
  translateLedgerView ::
    LedgerView protoFrom -> LedgerView protoTo
  translateChainDepState ::
    ChainDepState protoFrom -> ChainDepState protoTo

-- | Degenerate instance - we may always translate from a protocol to itself.
instance TranslateProto singleProto singleProto
  where
  translateConsensusConfig = id
  translateLedgerView = id
  translateChainDepState = id
