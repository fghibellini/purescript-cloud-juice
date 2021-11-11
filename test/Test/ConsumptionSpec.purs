module Test.ConsumptionSpec where

import Prelude

import Control.Monad.Reader (ReaderT, runReaderT)
import Data.Array (head)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..), fromJust)
import Data.Time.Duration (Milliseconds(..))
import Data.UUID (genUUID)
import Data.UUID as UUID
import Effect (Effect)
import Effect.AVar as AVar
import Effect.Aff (Aff, bracket, delay, error, makeAff, nonCanceler, throwError)
import Effect.Aff.Class (liftAff)
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Random (random)
import Effect.Ref as Ref
import FlowId (FlowId(..))
import Nakadi.Client (streamSubscriptionEvents)
import Nakadi.Client.Types (Env)
import Nakadi.Minimal as Minimal
import Nakadi.Types (Event(DataChangeEvent), SubscriptionId(..))
import Node.Express.App (all, listenHttp, post)
import Node.Express.Handler (Handler)
import Node.Express.Request (getMethod, getUrl)
import Node.Express.Response (end, setResponseHeader, setStatus)
import Node.HTTP (close)
import Node.Stream.Util (BufferSize(..))
import Partial.Unsafe (unsafePartial)
import Simple.JSON (class ReadForeign, read_, writeJSON)
import Test.Express.Curry as ExpressCurry
import Test.Spec (Spec, describe, itOnly)
import Test.Spec.Assertions (fail, shouldEqual)

nakadiPort :: Int
nakadiPort = 2323

env :: Env ()
env =
  { flowId: FlowId "test-flow-id"
  , baseUrl: "http://localhost"
  , token: pure "Bearer token"
  , port: nakadiPort
  , logWarn: \maybeProblem msg -> Console.log $ msg <> ": " <> show maybeProblem
  }

run :: ∀ m a. ReaderT (Env ()) m a -> m a
run = flip runReaderT env

-- Starts a mock Nakadi server
-- and returns an Aff action that can be run to terminate the server
--
-- 1. The consumer is fed batches containing a single event.
-- 2. The batches are sent with occasional pauses between them - this reproduces the
--    real-world behavior of Nakadi which will send you multiple batches right when you subscribe
-- 3. The server has an internal counter and is incremented with each batch sent.
--    This counter is included in the event data as the field `eventNr`
startMockNakadi :: Effect (Aff Unit)
startMockNakadi = do
    state <-
      { counterSent: _
      , counterReceived: _
      , completionSignal: _
      } <$> Ref.new 0 <*> Ref.new 0 <*> AVar.empty
    server <- listenHttp (app state) nakadiPort mempty
    let killServer = makeAff \cb -> close server (cb $ Right unit) $> nonCanceler
    pure killServer
  where
    app state = do
      let
        eventsToSend = 50
        likeliHoodOfPause = 0.2
      post "/subscriptions/46d0fc8c-fece-4b1d-9038-80e9b3b6a797/events" do
        setStatus 200
        setResponseHeader "X-Nakadi-StreamId" "abc-stream-id-def"
        let
          step :: Handler
          step = do
            n <- liftEffect $ Ref.modify' (\x -> { state: x + 1, value: x }) state.counterSent
            eid <- liftEffect genUUID
            ExpressCurry.write $ (_ <> "\n") $ writeJSON
              { cursor:
                { partition: "partition_007"
                , offset: "xxxx"
                , event_type: "yyy"
                , cursor_token: "cursor_token_" <> show n
                }
              , events:
                [ { data_type: "BLABLA"
                  , data_op: "C"
                  , metadata:
                    { eid: UUID.toString eid
                    , event_type: "yyy"
                    , occurred_at: "2021-11-11T19:00:00.000Z"
                    , received_at: "2021-11-11T19:00:00.000Z"
                    }
                  , data: { eventNr: n }
                  }
                ]
              }
            case (n + 1 >= eventsToSend) of
              true -> end
              false -> do
                whenM ((_ < likeliHoodOfPause) <$> liftEffect random) do
                  liftAff (delay (1000.0 # Milliseconds)) -- this mimics Nakadi waiting for the publisher to publish more events
                step
        step
      post "/subscriptions/46d0fc8c-fece-4b1d-9038-80e9b3b6a797/cursors" do
        setStatus 200 *> end
      all "*" do
        method <- getMethod
        url <- getUrl
        Console.error ("Unhandled request: " <> show method <> " " <> show url)
        setStatus 500 *> end

withNakadi :: forall a . Aff a -> Aff a
withNakadi aff =
  bracket
    (liftEffect startMockNakadi)
    (\fn -> fn)
    (\_ -> aff)

spec :: Spec Unit
spec =
  describe "Serial processing of event batches" $ do
    itOnly "Random pauses" $ do
      withNakadi do
        -- This test asserts that no batch starts being processed before the previous handler is finished.
        clientCounter <- liftEffect $ Ref.new 0
        -- `clientCounter` represents the local state that each batch handler works with.
        -- Each handler will increase this counter right before completing (after a random pause).
        -- The batches sent from Nakadi are tagged with a sequential number which should always match `clientCounter`.
        let
          handler events = do
            let event = unsafePartial $ fromJust $ head events -- the mock server always sends batches of 1 event
            eventCounter <- extractCounterFromEvent event
            expectedCounterValue <- liftEffect $ Ref.read clientCounter
            case compare expectedCounterValue eventCounter of
              LT -> fail ("received batch " <> show eventCounter <> " before processing " <> show expectedCounterValue)
              EQ -> pure unit -- Console.log "id is sequential."
              GT -> fail "received a batch that was processed already!"
            pause <- (\x -> Milliseconds (50.0 * x)) <$> liftEffect random
            delay pause
            liftEffect $ Ref.modify_ (_ + 1) clientCounter
        res <- run $ streamSubscriptionEvents
          (BufferSize $ 1024*1024)
          (SubscriptionId "46d0fc8c-fece-4b1d-9038-80e9b3b6a797")
          (Minimal.streamParameters 20 40)
          handler
        shouldEqual 50 =<< do
          delay (5000.0 # Milliseconds)
          liftEffect $ Ref.read clientCounter

extractCounterFromEvent :: Event -> Aff Int
extractCounterFromEvent event = do
  case (parseDataChangeEvent event :: _ { eventNr :: Int }) of
    Just n -> pure n.eventNr
    Nothing -> throwError $ error "Couldn't parse eventNr in Event!"
  where
    parseDataChangeEvent :: forall a. ReadForeign a => Event -> Maybe a
    parseDataChangeEvent = case _ of
      DataChangeEvent x -> read_ x.data
      _ -> Nothing

