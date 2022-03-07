module Test.Conflict409Spec
  ( env
  , extractCounterFromEvent
  , nakadiPort
  , spec
  , startMockNakadi
  , withNakadi
  )
  where

import Prelude

import Control.Monad.Reader (ReaderT, runReaderT)
import Data.Array (head)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..), fromJust)
import Data.UUID as UUID
import Effect (Effect)
import Effect.Aff (Aff, Milliseconds(..), bracket, error, makeAff, nonCanceler, throwError)
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Ref as Ref
import FlowId (FlowId(..))
import Nakadi.Client (streamSubscriptionEventsRetrying)
import Nakadi.Client.Types (Env)
import Nakadi.Minimal as Minimal
import Nakadi.Types (Event(DataChangeEvent), SubscriptionId(..))
import Node.Express.App (all, listenHttp, post)
import Node.Express.Request (getMethod, getUrl)
import Node.Express.Response (end, setResponseHeader, setStatus)
import Node.HTTP (close)
import Node.Stream.Util (BufferSize(..))
import Partial.Unsafe (unsafePartial)
import Simple.JSON (class ReadForeign, read_, writeJSON)
import Test.Express.Curry as ExpressCurry
import Test.Spec (Spec, describe, it)
import Test.Spec.Assertions (shouldEqual)


nakadiPort :: Int
nakadiPort = 2323

env :: Env ()
env =
  { flowId: FlowId "test-flow-id"
  , baseUrl: "http://localhost"
  , token: pure "Bearer token"
  , port: nakadiPort
  , timeout: Milliseconds 20000.0
  , logWarn: \maybeProblem msg -> Console.log $ msg <> ": " <> show maybeProblem
  }

run :: ∀ m a. ReaderT (Env ()) m a -> m a
run = flip runReaderT env

-- Starts a mock Nakadi server
-- and returns an Aff action that can be run to terminate the server
--
-- it returns a malformed response to the first consumer to subscribe
-- the next one will receive a single batch with the number 42
startMockNakadi :: Effect (Aff Unit)
startMockNakadi = do
    subscriptionCounter <- Ref.new 0
    server <- listenHttp (app subscriptionCounter) nakadiPort mempty
    let killServer = do
          makeAff \cb -> do
            close server (cb $ Right unit)
            pure nonCanceler
    pure killServer
  where
    app subscriptionCounter = do
      post "/subscriptions/46d0fc8c-fece-4b1d-9038-80e9b3b6a797/events" do
        consumptionId <- liftEffect $ Ref.modify' (\x -> { state: x + 1, value: x }) subscriptionCounter
        case consumptionId of
          0 -> do
            setStatus 409
            setResponseHeader "X-Nakadi-StreamId" "abc-stream-id-def"
            ExpressCurry.write "{\"title\":\"Conflict\",\"status\":409,\"detail\":\"No free slots for streaming available for subscription cf5d3743-cc7a-4c5f-acc6-aefafc747e3c. Total slots: 1\"}\n"
            end
          _ -> do
            setStatus 200
            setResponseHeader "X-Nakadi-StreamId" "abc-stream-id-def"
            eid <- liftEffect $ UUID.genUUID
            ExpressCurry.write $ (_ <> "\n") $ writeJSON
              { cursor:
                { partition: "partition_007"
                , offset: "xxxx"
                , event_type: "yyy"
                , cursor_token: "cursor_token_0"
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
                  , data: { eventNr: 42 }
                  }
                ]
              }
            end
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
  describe "Conflict response handling" $ do
    it "Should terminate upon receiving 409, and be able to resubscribe." $ do
      withNakadi do
        receivedNs <- liftEffect $ Ref.new []
        let
          handler events = do
            let event = unsafePartial $ fromJust $ head events -- the mock server always sends batches of 1 event
            eventN <- extractCounterFromEvent event
            liftEffect $ Ref.modify_ (_ <> [eventN]) receivedNs
        -- expected sequence of steps:
        -- 1. attempts to subscribe
        -- 2. receives 409 (no slots available)
        -- 3. will trigger a retry
        -- 4. receives a batch with a single event
        -- 5. Nakadi terminates the consumption stream
        res <- run $ streamSubscriptionEventsRetrying
          (BufferSize $ 1024*1024)
          (SubscriptionId "46d0fc8c-fece-4b1d-9038-80e9b3b6a797")
          (Minimal.streamParameters 20 40)
          handler
        shouldEqual [42] =<< do
          liftEffect $ Ref.read receivedNs

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

