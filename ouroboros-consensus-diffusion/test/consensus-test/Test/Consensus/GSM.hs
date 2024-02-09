{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StandaloneKindSignatures #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Consensus.GSM (tests) where

import           Control.Concurrent.Class.MonadSTM.Strict.TVar.Checked
import           Control.Monad.Class.MonadAsync (withAsync)
import           Control.Monad.Class.MonadFork (MonadFork, yield)
import           Control.Monad.Class.MonadSTM
import qualified Control.Monad.Class.MonadTime.SI as SI
import qualified Control.Monad.Class.MonadTimer.SI as SI
import           Control.Monad.IOSim (IOSim, runSimOrThrow)
import           Data.Kind (Type)
import           Data.List ((\\))
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import           Data.Time (diffTimeToPicoseconds)
import qualified Data.TreeDiff as TD
import           GHC.Generics (Generic, Generic1)
import qualified Ouroboros.Consensus.Node.GSM as GSM
import           Ouroboros.Network.PeerSelection.LedgerPeers.Type (LedgerStateJudgement (..))
import           Ouroboros.Consensus.Util.IOLike (IOLike)
import           Test.QuickCheck (choose, elements, oneof, shrink)
import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Monadic as QC
import           Test.StateMachine (Concrete, Symbolic)
import           Test.Consensus.IOSimQSM.Test.StateMachine.Sequential (runCommands')
import qualified Test.StateMachine as QSM
import qualified Test.StateMachine.Types as QSM
import qualified Test.StateMachine.Types.Rank2 as QSM
import           Test.Util.Orphans.IOLike ()
import           Test.Util.TestEnv (adjustQuickCheckTests)
import           Test.Util.ToExpr ()
import           Test.Tasty (TestTree)
import           Test.Tasty.QuickCheck (testProperty)

tests :: [TestTree]
tests = [
    adjustQuickCheckTests (* 100) $ testProperty "GSM" prop_sequential
  ]

-----

-- The following definitions are in the exact same order as the QSM tutorial at
-- <https://github.com/stevana/quickcheck-state-machine/blob/3748220bffacb61847e35e3808403f3e7187a7c6/README.md>.

-- | TODO restarts (or maybe just killing the GSM thread)
type Command :: (Type -> Type) -> Type
data Command r =
    Disconnect UpstreamPeer
    -- ^ INVARIANT must be an existing peer
    --
    -- Mocks the necessary ChainSync client behavior.
  |
    ExtendSelection S
    -- ^ INVARIANT 'selectionIsBehind'
    --
    -- NOTE Harmless to assume it only advances by @'B' 1@ at a time.
  |
    ModifyCandidate UpstreamPeer B
    -- ^ INVARIANT existing peer
    --
    -- Mocks the necessary ChainSync client behavior.
  |
    NewCandidate UpstreamPeer B
    -- ^ INVARIANT new peer
    --
    -- Mocks the necessary ChainSync client behavior.
  |
    ReadJudgment
  |
    ReadMarker
  |
    StartIdling UpstreamPeer
    -- ^ INVARIANT existing peer, not idling
  |
    TimePasses Int
    -- ^ tenths of a second
    --
    -- INVARIANT positive
    --
    -- INVARIANT does not end /exactly/ on an interesting time point; see
    -- 'boringDur'.
    --
    -- NOTE The generator does not yield consecutive 'TimePasses' commands,
    -- though shrinking might.
  deriving stock    (Generic1, Show)
  deriving anyclass (QSM.CommandNames, QSM.Foldable, QSM.Functor, QSM.Traversable)

type Response :: (Type -> Type) -> Type
data Response r =
    ReadThisJudgment LedgerStateJudgement
  |
    ReadThisMarker MarkerState
  |
    Unit
  deriving stock    (Generic1, Show)
  deriving anyclass (QSM.Foldable, QSM.Functor, QSM.Traversable)

semantics ::
     IOLike m
  => Vars m
  -> Command Concrete
  -> m (Response Concrete)
semantics vars = \case
    Disconnect peer -> do
        atomically $ do
            modifyTVar varCandidates $ Map.delete peer
            modifyTVar varIdlers     $ Set.delete peer
        pure Unit
    ExtendSelection sdel -> do
        atomically $ do
            Selection b s <- readTVar varSelection
            writeTVar varSelection $! Selection (b + 1) (s + sdel)
        pure Unit
    ModifyCandidate peer bdel -> do
        atomically $ do

            modifyTVar varIdlers $ Set.delete peer

            v <- (Map.! peer) <$> readTVar varCandidates
            Candidate b <- readTVar v
            writeTVar v $! Candidate (b + bdel)

        pure Unit
    NewCandidate peer bdel -> do
        atomically $ do
            Selection b _s <- readTVar varSelection
            v <- newTVar $! Candidate (b + bdel)
            modifyTVar varCandidates $ Map.insert peer v
        pure Unit
    ReadJudgment -> do
        yield10
        fmap ReadThisJudgment $ atomically $ readTVar varJudgment
    ReadMarker -> do
        yield10
        fmap ReadThisMarker $ atomically $ readTVar varMarker
    StartIdling peer -> do
        atomically $ modifyTVar varIdlers $ Set.insert peer
        pure Unit
    TimePasses dur -> do
        SI.threadDelay (0.1 * fromIntegral dur)
        pure Unit
  where
    Vars varSelection varCandidates varIdlers varJudgment varMarker = vars

-- | This is called before reading the implementation's state in order to
-- ensure that all pending STM transactions have commited before this read.
--
-- I'm unsure how many are actually necessary, but ten is both small and also
-- seems likely to suffice.
yield10 :: MonadFork m => m ()
yield10 =
    do yield5; yield5
  where
    yield5 = do yield; yield; yield; yield; yield

type Model :: (Type -> Type) -> Type
data Model r = Model {
    mCandidates :: Map.Map UpstreamPeer Candidate
  ,
    mClock :: SI.Time
  ,
    mIdlers :: Set.Set UpstreamPeer
  ,
    mPrev :: WhetherPrevTimePasses
  ,
    mSelection :: Selection
  ,
    mState :: ModelState
  }
  deriving (Generic, Show)
  deriving anyclass (TD.ToExpr)

initModel :: LedgerStateJudgement -> Model r
initModel j = Model {
    mCandidates = Map.empty
  ,
    mClock = SI.Time 0
  ,
    mIdlers = Set.empty
  ,
    mPrev = WhetherPrevTimePasses True
  ,
    mSelection = Selection 0 s
  ,
    mState = case j of
        TooOld      -> ModelTooOld
        YoungEnough -> ModelYoungEnough (SI.Time (-10000))
  }
  where
    s = S $ case j of
        TooOld      -> (-11)
        YoungEnough -> 0

precondition :: Model Symbolic -> Command Symbolic -> QSM.Logic
precondition model = \case
    ExtendSelection _sdel -> QSM.Boolean $
        selectionIsBehind model
    Disconnect peer -> QSM.Boolean $
        peer `Map.member` cands
    ModifyCandidate peer _bdel -> QSM.Boolean $
        peer `Map.member` cands
    NewCandidate peer _bdel -> QSM.Boolean $
        (not $ peer `Map.member` cands)
    ReadJudgment ->
        QSM.Top
    ReadMarker ->
        QSM.Top
    StartIdling peer -> QSM.Boolean $
        (peer `Map.member` cands)
     &&
        (peer `Set.notMember` idlers)
    TimePasses dur -> QSM.Boolean $
        (0 < dur)
     &&
        (boringDur model dur)
  where
    Model {
        mCandidates = cands
      ,
        mIdlers = idlers
      } = model

transition :: Model r -> Command r -> Response r -> Model r
transition model cmd resp = fixupModelState $ case (cmd, resp) of
    (Disconnect peer, Unit) ->
        model' {
            mCandidates = Map.delete peer cands
          ,
            mIdlers = Set.delete peer idlers
          }
    (ExtendSelection sdel, Unit) ->
        model' { mSelection = Selection (b + 1) (s + sdel) }
    (ModifyCandidate peer bdel, Unit) ->
        model' {
            mCandidates = Map.insertWith plusC peer (Candidate bdel) cands
          ,
            mIdlers = Set.delete peer idlers
          }
    (NewCandidate peer bdel, Unit) ->
        model' { mCandidates = Map.insert peer (Candidate (b + bdel)) cands }
    (ReadJudgment, ReadThisJudgment{}) ->
        model'
    (ReadMarker, ReadThisMarker{}) ->
        model'
    (StartIdling peer, Unit) ->
        model' { mIdlers = Set.insert peer idlers }
    (TimePasses dur, Unit) ->
        model {
            mClock = SI.addTime (0.1 * fromIntegral dur) clk
          ,
            mPrev = WhetherPrevTimePasses True
          }
    o -> error $ "impossible response: " <> show o
  where
    Model {
        mCandidates = cands
      ,
        mClock = clk
      ,
        mIdlers = idlers
      ,
        mSelection = Selection b s
      } = model

    model' = model { mPrev = WhetherPrevTimePasses False }

    plusC (Candidate x) (Candidate y) = Candidate (x + y)

postcondition ::
     Model Concrete
  -> Command Concrete
  -> Response Concrete
  -> QSM.Logic
postcondition model _cmd = \case
    ReadThisJudgment j' ->
        j' QSM..== j
    ReadThisMarker m' ->
        m' QSM..== toMarker j
    Unit ->
        QSM.Top
  where
    j = toJudgment $ mState model

generator :: Model Symbolic -> Maybe (QC.Gen (Command Symbolic))
generator model = Just $ QC.frequency $
    [ (,) 5 $ Disconnect <$> elements old | notNull old ]
 <>
    -- NB harmless to assume this node never mints
    [ (,) 10 $ ExtendSelection <$> schoose (-4) 10 | selectionIsBehind model ]
 <>
    [ (,) 20 $ ModifyCandidate <$> elements old <*> bchoose (-1) 5 | notNull old ]
 <>
    [ (,) 100 $
            NewCandidate
        <$> elements new
        <*> (B <$> choose (-10, 10))
    | notNull new
    ]
 <>
    [ (,) 20 $ pure ReadJudgment ]
 <>
    [ (,) 20 $ pure ReadMarker ]
 <>
    [ (,) 50 $ StartIdling <$> elements oldNotIdling | notNull oldNotIdling ]
 <>
    [ (,) 100 $ TimePasses <$> choose (1, 70) | prev == WhetherPrevTimePasses False ]
  where
    Model {
        mCandidates = cands
      ,
        mIdlers = idlers
      ,
        mPrev = prev
      } = model

    notNull :: [a] -> Bool
    notNull = not . null

    old = Map.keys cands

    new = [ minBound .. maxBound ] \\ old

    oldNotIdling = old \\ Set.toList idlers

    bchoose u v = B <$> oneof [ choose (u, (-1)), choose (1, v) ]
    schoose u v = S <$> oneof [ choose (u, (-1)), choose (1, v) ]

shrinker :: Model Symbolic -> Command Symbolic -> [Command Symbolic]
shrinker _model = \case
    Disconnect{} ->
        []
    ExtendSelection sdel ->
        [ ExtendSelection sdel' | sdel' <- shrinkS sdel ]
    ModifyCandidate peer bdel ->
        [ ModifyCandidate peer bdel' | bdel' <- shrinkB bdel, bdel' /= 0 ]
    NewCandidate peer bdel ->
        [ NewCandidate peer bdel' | bdel' <- shrinkB bdel, bdel' /= 0  ]
    ReadJudgment ->
        []
    ReadMarker ->
        []
    StartIdling{} ->
        []
    TimePasses dur ->
        [ TimePasses dur' | dur' <- shrink dur, 0 < dur' ]
  where
    shrinkB (B x) = [ B x' | x' <- shrink x ]
    shrinkS (S x) = [ S x' | x' <- shrink x ]

mock :: Model Symbolic -> Command Symbolic -> QSM.GenSym (Response Symbolic)
mock model = pure . \case
    Disconnect{} ->
        Unit
    ExtendSelection{} ->
        Unit
    ModifyCandidate{} ->
        Unit
    NewCandidate{} ->
        Unit
    ReadJudgment ->
        ReadThisJudgment $ j
    ReadMarker ->
        ReadThisMarker $ toMarker j
    StartIdling{} ->
        Unit
    TimePasses{} ->
        Unit
  where
    j = toJudgment $ mState model

sm ::
     IOLike m
  => Vars m
  -> LedgerStateJudgement
  -> QSM.StateMachine Model Command m Response
sm vars j = QSM.StateMachine {
    QSM.cleanup = \_model -> pure ()
  ,
    QSM.generator = generator
  ,
    QSM.initModel = initModel j
  ,
    QSM.invariant = Nothing
  ,
    QSM.mock = mock
  ,
    QSM.postcondition = postcondition
  ,
    QSM.precondition = precondition
  ,
    QSM.semantics = semantics vars
  ,
    QSM.shrinker = shrinker
  ,
    QSM.transition = transition
  }

prop_sequential ::
     LedgerStateJudgement
  -> QC.Property
prop_sequential j0 =
    QSM.forAllCommands
        (sm (undefined :: Vars IO) j0)   -- NB the specific IO type is unused here
        mbMinimumCommandLen
        predicate
  where
    mbMinimumCommandLen = Just 20

    predicate :: QSM.Commands Command Response -> QC.Property
    predicate cmds = runSimOrThrow $ do
        varSelection  <- newTVarIO (mSelection $ initModel j0)
        varCandidates <- newTVarIO Map.empty
        varIdlers     <- newTVarIO Set.empty
        varJudgment   <- newTVarIO j0
        varMarker     <- newTVarIO (toMarker j0)
        let vars =
                Vars
                    varSelection
                    varCandidates
                    varIdlers
                    varJudgment
                    varMarker
        let sm' = sm vars j0
        let gsm = GSM.realGsmEntryPoints GSM.GsmView {
                GSM.antiThunderingHerd = Nothing
              ,
                GSM.candidateOverSelection = candidateOverSelection
              ,
                GSM.durationUntilTooOld = Just durationUntilTooOld
              ,
                GSM.equivalent = (==)   -- unsound, but harmless in this test
              ,
                GSM.getChainSyncCandidates = readTVar varCandidates
              ,
                GSM.getChainSyncIdlers = readTVar varIdlers
              ,
                GSM.getCurrentSelection = readTVar varSelection
              ,
                GSM.minCaughtUpDuration = thrashLimit
              ,
                GSM.setCaughtUpPersistentMark = \b ->
                    atomically $ do
                        writeTVar varMarker $ if b then Present else Absent
              ,
                GSM.varLedgerStateJudgement = varJudgment
              }
            gsmEntryPoint = case j0 of
                TooOld      -> GSM.enterOnlyBootstrap gsm
                YoungEnough -> GSM.enterCaughtUp      gsm

        (hist, _model, res) <- id
          $ withAsync gsmEntryPoint
          $ \_async -> runCommands' (pure sm') cmds

        pure
          $ QC.monadicIO
          $ QSM.prettyCommands (sm undefined j0) hist
          $ QSM.checkCommandNames cmds (res QC.=== QSM.Ok)

-----

-- | A block count
newtype B = B Int
  deriving stock    (Eq, Ord, Generic, Show)
  deriving newtype  (Num)
  deriving anyclass (TD.ToExpr)

-- | A slock count
newtype S = S Int
  deriving stock    (Eq, Ord, Generic, Show)
  deriving newtype  (Num)
  deriving anyclass (TD.ToExpr)

data UpstreamPeer = Amara | Bao | Cait | Dhani | Eric
  deriving stock    (Bounded, Enum, Eq, Ord, Generic, Show)
  deriving anyclass (TD.ToExpr)

-- | The cumulative growth relative to whatever length the initial selection
-- was and the slot relative to the start of the test (which is assume to be
-- the exact onset of some slot)
data Selection = Selection !B !S
  deriving stock    (Eq, Ord, Generic, Show)
  deriving anyclass (TD.ToExpr)

-- | The age of the candidate is irrelevant, only its length matters
newtype Candidate = Candidate B
  deriving stock    (Eq, Ord, Generic, Show)
  deriving anyclass (TD.ToExpr)

data MarkerState = Present | Absent
  deriving stock    (Eq, Ord, Generic, Show)
  deriving anyclass (TD.ToExpr)

newtype WhetherPrevTimePasses = WhetherPrevTimePasses Bool
  deriving stock    (Eq, Ord, Generic, Show)
  deriving anyclass (TD.ToExpr)

data ModelState =
    ModelTooOld
  |
    ModelYoungEnough !SI.Time
    -- ^ when the model most recently transitioned to 'YoungEnough'
  deriving stock    (Eq, Ord, Generic, Show)
  deriving anyclass (TD.ToExpr)

candidateOverSelection ::
     Selection
  -> Candidate
  -> GSM.CandidateVersusSelection
candidateOverSelection (Selection b _s) (Candidate b') =
    -- TODO this ignores CandidateDoesNotIntersect, which seems harmless, but
    -- I'm not quite sure
    GSM.WhetherCandidateIsBetter (b < b')

durationUntilTooOld :: Selection -> IOSim s GSM.DurationFromNow
durationUntilTooOld sel = do
    let t = ageLimit `SI.addTime` onset sel
    now <- SI.getMonotonicTime
    pure
      $ if t <= now then GSM.Already else
        GSM.After $ realToFrac $ t `SI.diffTime` now

toJudgment :: ModelState -> LedgerStateJudgement
toJudgment = \case
    ModelTooOld        -> TooOld
    ModelYoungEnough{} -> YoungEnough

toMarker :: LedgerStateJudgement -> MarkerState
toMarker = \case
    TooOld      -> Absent
    YoungEnough -> Present

-- | Update the 'mState', assuming that's the only stale field in the given
-- 'Model'
fixupModelState :: Model r -> Model r
fixupModelState model =
    case st of
        ModelTooOld | caughtUp ->
            -- ASSUMPTION This new state was /NOT/ incurred by the 'TimePasses'
            -- command.
            --
            -- Therefore the current clock is necessarily the correct timestamp
            -- to record.
            model { mState = ModelYoungEnough clk }
        ModelYoungEnough timestamp | fellBehind timestamp ->
            -- ASSUMPTION The 'TimePasses' command incurred this new state.
            --
            -- It's possible for the node to instantly return to CaughtUp, but
            -- that might have happened /during/ the 'TimePasses' command, not
            -- only when it ends.
            --
            -- Therefore the age limit of the selection is the correct
            -- timestamp to record, instead of the current clock (ie when the
            -- 'TimePasses' ended).
            --
            -- NOTE Superficially, in the real implementation, the Diffusion
            -- Layer should be discarding all peers when transitioning from
            -- CaughtUp to OnlyBootstrap. However, it would be plausible for an
            -- implementation to retain any bootstrap peers it happened to
            -- have, so the idiosyncratic behavior of the system under test in
            -- this module is not totally irrelevant.
            if caughtUp
            then model { mState = ModelYoungEnough (timestamp' timestamp) }
            else model { mState = ModelTooOld }
        _ ->
            model
  where
    Model {
        mCandidates = cands
      ,
        mClock = clk
      ,
        mIdlers = idlers
      ,
        mSelection = sel
      ,
        mState = st
      } = model

    caughtUp             = some && allIdling && all ok cands
    fellBehind timestamp = tooOld && notThrashing timestamp

    some = 0 < Map.size cands

    allIdling = idlers == Map.keysSet cands

    ok cand =
        GSM.WhetherCandidateIsBetter False == candidateOverSelection sel cand

    fortiethBirthday = SI.addTime ageLimit (onset sel)
    tooOld           = fortiethBirthday < clk   -- NB 'boringDur' prevents equivalence

    release      timestamp = SI.addTime thrashLimit timestamp
    notThrashing timestamp = release timestamp < clk   -- NB 'boringDur' prevents equivalence

    -- The /last/ time the node instantaneously visited OnlyBootstrap during
    -- the 'TimePasses' command.
    timestamp' timestamp =
        foldl max fortiethBirthday
      $ filter (< clk)   -- NB 'boringDur' prevents equivalence
      $ iterate (SI.addTime thrashLimit) timestamp

selectionIsBehind :: Model r -> Bool
selectionIsBehind model =
    any (\(Candidate b') -> b' > b) cands
  where
    Model {
        mCandidates = cands
      ,
        mSelection = Selection b _s
      } = model

onset :: Selection -> SI.Time
onset (Selection _b (S s)) = SI.Time $ 0.1 * fromIntegral s

ageLimit :: Num a => a
ageLimit = 10   -- seconds

thrashLimit :: Num a => a
thrashLimit = 10   -- seconds

-- | Checks that a 'TimePasses' command does not end exactly when a timeout
-- could fire
--
-- This insulates the test from race conditions that are innocuous in the real
-- world.
boringDur :: Model r -> Int -> Bool
boringDur model dur =
    boringSelection && boringState
  where
    Model {
        mClock = clk
      ,
        mSelection = sel
      ,
        mState = st
      } = model

    clk' = SI.addTime (0.1 * fromIntegral dur) clk

    boringSelection = clk' /= SI.addTime ageLimit (onset sel)

    boringState = case st of
        ModelTooOld                -> True
        ModelYoungEnough timestamp ->
            let gap = clk' `SI.diffTime` SI.addTime thrashLimit timestamp
            in
            0 /= (diffTimeToPicoseconds gap `mod` secondsToPicoseconds thrashLimit)

    secondsToPicoseconds x = x * (1000 * 1000 * 1000)

-----

data Vars m = Vars
    (StrictTVar m Selection)
    (StrictTVar m (Map.Map UpstreamPeer (StrictTVar m Candidate)))
    (StrictTVar m (Set.Set UpstreamPeer))
    (StrictTVar m LedgerStateJudgement)
    (StrictTVar m MarkerState)

instance QC.Arbitrary LedgerStateJudgement where
    arbitrary = elements [TooOld, YoungEnough]
    shrink    = \case
        TooOld      -> [YoungEnough]
        YoungEnough -> []

instance QC.Arbitrary MarkerState where
    arbitrary = elements [Absent, Present]
    shrink    = \case
        Absent  -> [Present]
        Present -> []


instance TD.ToExpr SI.Time              where toExpr = TD.defaultExprViaShow
instance TD.ToExpr LedgerStateJudgement where toExpr = TD.defaultExprViaShow

-----

-- TODO 'realMarkerFileView' unit tests.

-- TODO 'initializationLedgerJudgement' tests.

-- TODO 'realDurationUntilTooOld' tests.

-- TODO The ChainSync client should signal idle exactly when and only when the
-- MsgAwaitReply is processed.

-- TODO The ChainSync client subsequently should signal no-longer-idle exactly
-- when and only when the next message is received.
