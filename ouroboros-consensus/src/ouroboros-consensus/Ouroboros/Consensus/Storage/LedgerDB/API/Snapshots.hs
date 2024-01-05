{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- | Common logic and types for LedgerDB Snapshots.

module Ouroboros.Consensus.Storage.LedgerDB.API.Snapshots (
    -- * Disk policy
    DiskPolicy (..)
  , defaultDiskPolicy
    -- * Snapshots
  , DiskSnapshot (..)
  , SnapshotFailure (..)
    -- * Codec
  , readExtLedgerState
  , writeExtLedgerState
    -- * Paths
  , diskSnapshotIsTemporary
  , snapshotToDirName
  , snapshotToDirPath
    -- * Management
  , deleteSnapshot
  , listSnapshots
  , trimSnapshots
    -- * Tracing
  , TraceSnapshotEvent (..)
  ) where

import           Codec.CBOR.Decoding
import           Codec.CBOR.Encoding
import qualified Codec.CBOR.Write as CBOR
import qualified Codec.Serialise.Decoding as Dec
import           Control.Monad
import           Control.Monad.Except
import           Control.Tracer
import qualified Data.List as List
import           Data.Maybe (isJust, mapMaybe)
import           Data.Ord
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word
import           GHC.Generics
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Storage.LedgerDB.API.DiskPolicy
import           Ouroboros.Consensus.Util.CallStack
import           Ouroboros.Consensus.Util.CBOR (ReadIncrementalErr,
                     decodeWithOrigin, readIncremental)
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.Versioned
import           System.FS.API
import           System.FS.API.Types
import           Text.Read (readMaybe)

data DiskSnapshot = DiskSnapshot {
      -- | Snapshots are numbered. We will try the snapshots with the highest
      -- number first.
      --
      -- When creating a snapshot, we use the slot number of the ledger state it
      -- corresponds to as the snapshot number. This gives an indication of how
      -- recent the snapshot is.
      --
      -- Note that the snapshot names are only indicative, we don't rely on the
      -- snapshot number matching the slot number of the corresponding ledger
      -- state. We only use the snapshots numbers to determine the order in
      -- which we try them.
      dsNumber :: Word64

      -- | Snapshots can optionally have a suffix, separated by the snapshot
      -- number with an underscore, e.g., @4492799_last_Byron@. This suffix acts
      -- as metadata for the operator of the node. Snapshots with a suffix will
      -- /not be trimmed/.
    , dsSuffix :: Maybe String
    }
  deriving (Show, Eq, Ord, Generic)

data SnapshotFailure blk =
    -- | We failed to deserialise the snapshot
    --
    -- This can happen due to data corruption in the ledger DB.
    InitFailureRead ReadIncrementalErr

    -- | This snapshot is too recent (ahead of the tip of the immutable chain)
  | InitFailureTooRecent (RealPoint blk)

    -- | This snapshot was of the ledger state at genesis, even though we never
    -- take snapshots at genesis, so this is unexpected.
  | InitFailureGenesis
  deriving (Show, Eq, Generic)

-- | Named snapshot are permanent, they will never be deleted when trimming.
diskSnapshotIsPermanent :: DiskSnapshot -> Bool
diskSnapshotIsPermanent = isJust . dsSuffix

-- | The snapshots that are periodically created are temporary, they will be
-- deleted when trimming
diskSnapshotIsTemporary :: DiskSnapshot -> Bool
diskSnapshotIsTemporary = not . diskSnapshotIsPermanent

snapshotFromPath :: String -> Maybe DiskSnapshot
snapshotFromPath fileName = do
    number <- readMaybe prefix
    return $ DiskSnapshot number suffix'
  where
    (prefix, suffix) = break (== '_') fileName

    suffix' :: Maybe String
    suffix' = case suffix of
      ""      -> Nothing
      _ : str -> Just str

-- | List on-disk snapshots, highest number first.
listSnapshots :: Monad m => SomeHasFS m -> m [DiskSnapshot]
listSnapshots (SomeHasFS HasFS{listDirectory}) =
    aux <$> listDirectory (mkFsPath [])
  where
    aux :: Set String -> [DiskSnapshot]
    aux = List.sortOn (Down . dsNumber) . mapMaybe snapshotFromPath . Set.toList

-- | Delete snapshot from disk
deleteSnapshot :: HasCallStack => SomeHasFS m -> DiskSnapshot -> m ()
deleteSnapshot (SomeHasFS HasFS{removeDirectoryRecursive}) =
  removeDirectoryRecursive . snapshotToDirPath

-- | Read an extended ledger state from disk
readExtLedgerState ::
     forall m blk. IOLike m
  => SomeHasFS m
  -> (forall s. Decoder s (ExtLedgerState blk EmptyMK))
  -> (forall s. Decoder s (HeaderHash blk))
  -> FsPath
  -> ExceptT ReadIncrementalErr m (ExtLedgerState blk EmptyMK)
readExtLedgerState hasFS decLedger decHash = do
      ExceptT
    . readIncremental hasFS decoder
  where
    decoder :: Decoder s (ExtLedgerState blk EmptyMK)
    decoder = decodeLBackwardsCompatible (Proxy @blk) decLedger decHash

-- | Write an extended ledger state to disk
writeExtLedgerState ::
     forall m blk. MonadThrow m
  => SomeHasFS m
  -> (ExtLedgerState blk EmptyMK -> Encoding)
  -> FsPath
  -> ExtLedgerState blk EmptyMK
  -> m ()
writeExtLedgerState (SomeHasFS hasFS) encLedger path cs = do
    withFile hasFS path (WriteMode MustBeNew) $ \h ->
      void $ hPut hasFS h $ CBOR.toBuilder (encoder cs)
  where
    encoder :: ExtLedgerState blk EmptyMK -> Encoding
    encoder = encodeL encLedger

-- | Trim the number of on disk snapshots so that at most 'onDiskNumSnapshots'
-- snapshots are stored on disk. The oldest snapshots are deleted.
--
-- The deleted snapshots are returned.
trimSnapshots ::
     Monad m
  => Tracer m (TraceSnapshotEvent r)
  -> SomeHasFS m
  -> DiskPolicy
  -> m [DiskSnapshot]
trimSnapshots tracer hasFS DiskPolicy{onDiskNumSnapshots} = do
    -- We only trim temporary snapshots
    diskSnapshots <- filter diskSnapshotIsTemporary <$> listSnapshots hasFS
    -- The snapshot are most recent first, so we can simply drop from the
    -- front to get the snapshots that are "too" old.
    forM (drop (fromIntegral onDiskNumSnapshots) diskSnapshots) $ \snapshot -> do
      deleteSnapshot hasFS snapshot
      traceWith tracer $ DeletedSnapshot snapshot
      return snapshot

snapshotToDirName :: DiskSnapshot -> String
snapshotToDirName DiskSnapshot { dsNumber, dsSuffix } =
    show dsNumber <> suffix
  where
    suffix = case dsSuffix of
      Nothing -> ""
      Just s  -> "_" <> s

-- | The path within the LedgerDB's filesystem to the snapshot's directory
snapshotToDirPath :: DiskSnapshot -> FsPath
snapshotToDirPath = mkFsPath . (:[]) . snapshotToDirName

-- | Version 1: uses versioning ('Ouroboros.Consensus.Util.Versioned') and only
-- encodes the ledger state @l@.
snapshotEncodingVersion1 :: VersionNumber
snapshotEncodingVersion1 = 1

-- | Encoder to be used in combination with 'decodeSnapshotBackwardsCompatible'.
encodeL :: (l -> Encoding) -> l -> Encoding
encodeL encodeLedger l =
    encodeVersion snapshotEncodingVersion1 (encodeLedger l)

-- | To remain backwards compatible with existing snapshots stored on disk, we
-- must accept the old format as well as the new format.
--
-- The old format:
--
-- * The tip: @WithOrigin (RealPoint blk)@
--
-- * The chain length: @Word64@
--
-- * The ledger state: @l@
--
-- The new format is described by 'snapshotEncodingVersion1'.
--
-- This decoder will accept and ignore them. The encoder ('encodeSnapshot') will
-- no longer encode them.
decodeLBackwardsCompatible ::
     forall l blk.
     Proxy blk
  -> (forall s. Decoder s l)
  -> (forall s. Decoder s (HeaderHash blk))
  -> forall s. Decoder s l
decodeLBackwardsCompatible _ decodeLedger decodeHash =
    decodeVersionWithHook
      decodeOldFormat
      [(snapshotEncodingVersion1, Decode decodeVersion1)]
  where
    decodeVersion1 :: forall s. Decoder s l
    decodeVersion1 = decodeLedger

    decodeOldFormat :: Maybe Int -> forall s. Decoder s l
    decodeOldFormat (Just 3) = do
        _ <- withOriginRealPointToPoint <$>
               decodeWithOrigin (decodeRealPoint @blk decodeHash)
        _ <- Dec.decodeWord64
        decodeLedger
    decodeOldFormat mbListLen =
        fail $
          "decodeSnapshotBackwardsCompatible: invalid start " <>
          show mbListLen

{-------------------------------------------------------------------------------
  Tracing snapshot events
-------------------------------------------------------------------------------}

data TraceSnapshotEvent blk
  = InvalidSnapshot DiskSnapshot (SnapshotFailure blk)
    -- ^ An on disk snapshot was skipped because it was invalid.
  | TookSnapshot DiskSnapshot (RealPoint blk)
    -- ^ A snapshot was written to disk.
  | DeletedSnapshot DiskSnapshot
    -- ^ An old or invalid on-disk snapshot was deleted
  deriving (Generic, Eq, Show)
