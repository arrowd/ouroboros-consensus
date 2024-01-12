{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE EmptyDataDeriving          #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE QuantifiedConstraints      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE ViewPatterns               #-}
-- |

{-# OPTIONS_GHC -Wno-orphans #-}

module Ouroboros.Consensus.Storage.LedgerDB.V2.InMemory (
    -- * LedgerTablesHandle
    newInMemoryLedgerTablesHandle
    -- * Snapshots
  , loadSnapshot
  , takeSnapshot
  ) where

import           Cardano.Binary as CBOR
import qualified Codec.CBOR.Read as CBOR
import qualified Codec.CBOR.Write as CBOR
import           Codec.Serialise (decode)
import           Control.Monad (unless, void)
import           Control.Monad.Except
import           Control.Tracer
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import           Data.String (fromString)
import           GHC.Generics
import           NoThunks.Class
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.SupportsProtocol
import           Ouroboros.Consensus.Ledger.Tables.Utils
import           Ouroboros.Consensus.Storage.LedgerDB.API
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Args
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Flavors
import           Ouroboros.Consensus.Storage.LedgerDB.Impl.Snapshots
import           Ouroboros.Consensus.Storage.LedgerDB.V2.LedgerSeq
import           Ouroboros.Consensus.Util.IOLike
import           Prelude hiding (read)
import           System.FS.API
import           System.FS.API.Types

{-------------------------------------------------------------------------------
  InMemory implementation of LedgerTablesHandles
-------------------------------------------------------------------------------}

data LedgerTablesHandleState l =
    LedgerTablesHandleOpen (LedgerTables l ValuesMK)
  | LedgerTablesHandleClosed
  deriving Generic

deriving instance NoThunks (LedgerTables l ValuesMK) => NoThunks (LedgerTablesHandleState l)

guardClosed :: LedgerTablesHandleState l -> (LedgerTables l ValuesMK -> a) -> a
guardClosed LedgerTablesHandleClosed    _ = error "Closed"
guardClosed (LedgerTablesHandleOpen st) f = f st

newInMemoryLedgerTablesHandle ::
     ( IOLike m
     , HasLedgerTables l
     , CanSerializeLedgerTables l
     )
  => LedgerTables l ValuesMK
  -> m (LedgerTablesHandle m l)
newInMemoryLedgerTablesHandle l = do
  ioref <- newTVarIO (LedgerTablesHandleOpen l)
  pure LedgerTablesHandle {
      close = atomically $ modifyTVar ioref (const LedgerTablesHandleClosed)
    , duplicate = do
        hs <- readTVarIO ioref
        guardClosed hs newInMemoryLedgerTablesHandle
    , read = \keys -> do
        hs <- readTVarIO ioref
        guardClosed hs (\st -> pure $ ltliftA2 rawRestrictValues st keys)
    , readAll = do
        hs <- readTVarIO ioref
        guardClosed hs pure
    , write = \diffs -> do
        atomically
        $ modifyTVar ioref
        (`guardClosed` (\st -> LedgerTablesHandleOpen (ltliftA2 rawApplyDiffs st diffs)))
    , writeToDisk = \(SomeHasFS hasFS) path -> do
        createDirectory hasFS path
        h <- readTVarIO ioref
        guardClosed h $
          \values ->
            withFile hasFS (extendPath path) (WriteMode MustBeNew) $ \hf ->
              void $ hPutAll hasFS hf
                   $ CBOR.toLazyByteString
                   $ valuesMKEncoder values
    , tablesSize = do
        hs <- readTVarIO ioref
        guardClosed hs (\(getLedgerTables -> ValuesMK m) -> pure $ Map.size m)
    }

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- | The path within the LedgerDB's filesystem to the file that contains the
-- snapshot's serialized ledger state
snapshotToStatePath :: DiskSnapshot -> FsPath
snapshotToStatePath = mkFsPath . (\x -> [x, "state"]) . snapshotToDirName

-- | The path within the LedgerDB's filesystem to the directory that contains a
-- the serialized tables.
snapshotToTablesPath :: DiskSnapshot -> FsPath
snapshotToTablesPath = mkFsPath . (\x -> [x, "tables"]) . snapshotToDirName

writeSnapshot ::
     MonadThrow m
  => SomeHasFS m
  -> (ExtLedgerState blk EmptyMK -> Encoding)
  -> DiskSnapshot
  -> StateRef m (ExtLedgerState blk)
  -> m ()
writeSnapshot fs encLedger ds st = do
    writeExtLedgerState fs encLedger (snapshotToDirPath ds) $ state st
    writeToDisk (tables st) fs (extendPath $ snapshotToTablesPath ds)

takeSnapshot ::
     ( MonadThrow m
     , LedgerDbSerialiseConstraints blk
     , LedgerSupportsProtocol blk
     )
  => CodecConfig blk
  -> Tracer m (TraceSnapshotEvent blk)
  -> SomeHasFS m
  -> StateRef m (ExtLedgerState blk)
  -> m (Maybe (DiskSnapshot, RealPoint blk))
takeSnapshot ccfg tracer hasFS st = do
  case pointToWithOriginRealPoint (castPoint (getTip $ state st)) of
    Origin -> return Nothing
    NotOrigin t -> do
      let number   = unSlotNo (realPointSlot t)
          snapshot = DiskSnapshot number Nothing
      diskSnapshots <- listSnapshots hasFS
      if List.any ((== number) . dsNumber) diskSnapshots then
        return Nothing
        else do
        writeSnapshot hasFS (encodeExtLedgerState' ccfg) snapshot st
        traceWith tracer $ TookSnapshot snapshot t
        return $ Just (snapshot, t)

extendPath :: FsPath -> FsPath
extendPath path =
      fsPathFromList $ fsPathToList path <> [fromString "tvar"]

loadSnapshot ::
    ( LedgerDbSerialiseConstraints blk
    , LedgerSupportsProtocol blk
    , IOLike m
    )
    => CodecConfig blk
    -> SomeHasFS m
    -> DiskSnapshot
    -> m (Either (SnapshotFailure blk) (LedgerSeq' m blk, RealPoint blk))
loadSnapshot ccfg fs@(SomeHasFS hasFS) ds = do
  eExtLedgerSt <- runExceptT $ readExtLedgerState fs (decodeExtLedgerState' ccfg) decode (snapshotToStatePath ds)
  case eExtLedgerSt of
    Left err -> pure (Left $ InitFailureRead err)
    Right extLedgerSt -> do
      case pointToWithOriginRealPoint (castPoint (getTip extLedgerSt)) of
        Origin        -> pure (Left InitFailureGenesis)
        NotOrigin pt -> do
          values <- withFile hasFS (extendPath (snapshotToTablesPath ds)) ReadMode $ \h -> do
            bs <- hGetAll hasFS h
            case CBOR.deserialiseFromBytes valuesMKDecoder bs of
              Left  err        -> error $ show err
              Right (extra, x) -> do
                unless (BSL.null extra) $ error "Trailing bytes in snapshot"
                pure x
          Right . (,pt) <$> empty extLedgerSt values newInMemoryLedgerTablesHandle

{-------------------------------------------------------------------------------
  Traces
-------------------------------------------------------------------------------}

data instance FlavorImplSpecificTrace FlavorV2 InMemory deriving (Eq, Show)
