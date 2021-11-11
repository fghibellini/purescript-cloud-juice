module Test.ApiSpec where

import Prelude

import Control.Monad.Except (ExceptT(..), runExceptT)
import Control.Monad.Reader (ReaderT, runReaderT)
import Data.Array as Array
import Data.Bifunctor (lmap)
import Data.DateTime.Instant (Instant, unInstant)
import Data.Either (Either(..), hush)
import Data.Int (round, toNumber)
import Data.List.Lazy (toUnfoldable)
import Data.List.Lazy as List
import Data.List.Lazy.NonEmpty as NEL
import Data.Maybe (fromMaybe)
import Data.Newtype (unwrap)
import Data.String as String
import Data.Time.Duration (Milliseconds(..), Seconds(..), convertDuration)
import Data.Traversable (sequence_, traverse_)
import Data.Variant (expand)
import Effect.Aff (Aff, delay)
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Now (now)
import Effect.Ref (Ref)
import Effect.Ref as Ref
import FlowId (FlowId(..))
import Nakadi.Client (deleteEventType, getEventType, getEventTypes, postEventType, postEvents, postSubscription, putEventType, streamSubscriptionEvents)
import Nakadi.Client.Types (Env)
import Nakadi.Minimal as Minimal
import Nakadi.Types (Event(..), EventTypeName(..), OwningApplication(..), SubscriptionId(..))
import Node.Encoding (Encoding(..))
import Node.Process (stdout)
import Node.Stream (writeString)
import Node.Stream.Util (BufferSize(..))
import Simple.JSON (class WriteForeign, writeImpl, writeJSON)
import Test.Spec (Spec, describe, it, pending)
import Test.Spec.Assertions (fail, shouldEqual)


env :: Env ()
env =
  { flowId: FlowId "test-flow-id"
  , baseUrl: "http://localhost"
  , token: pure "Bearer token"
  , port: 8080
  , logWarn: \maybeProblem msg -> Console.log $ msg <> ": " <> show maybeProblem
  }

run :: ∀ m a. ReaderT (Env ()) m a -> m a
run = flip runReaderT env

spec :: Spec Unit
spec =
  describe "api" $ do
    let undefinedET = Minimal.eventType
          (EventTypeName "test-undefined")
          (OwningApplication "cloud-juice")
          (Minimal.eventTypeSchema)
    let undefinedETN = undefinedET # _.name

    describe "/event-types" $ do
      it "Preparation" $ do
        res <- run <<< runExceptT $ do
          types <- ExceptT $ (map <<< lmap $ expand) getEventTypes
          traverse_ (ExceptT <<< deleteEventType) (types <#> _.name)
        res `shouldEqual` (Right unit)
      it "POST" $ do
        res <- run $ postEventType undefinedET
        res `shouldEqual` (Right unit)
      it "GET" $ do
        res <- run getEventTypes
        (res <#> (map _.name)) `shouldEqual` (Right [undefinedETN])

    describe "/event-types/{name}" $ do
      it "GET" $ do
        et <- run $ getEventType undefinedETN
        (et <#> _.name) `shouldEqual` (Right undefinedETN)
      it "PUT" $ do
        let modified = undefinedET { owning_application = OwningApplication "other-app" }
        put <- run $ putEventType undefinedETN modified
        put `shouldEqual` (Right unit)
        get <- run $ getEventType undefinedETN
        (get <#> _.owning_application) `shouldEqual` (Right modified.owning_application)
      it "DELETE" $ do
        res <- run $ deleteEventType undefinedETN
        res `shouldEqual` (Right unit)

    describe "/event-types/{name}/cursor-distances" $ do
      pending "POST"
    describe "/event-types/{name}/cursors-lag" $ do
      pending "POST"
    describe "/event-types/{name}/events" $ do
      -- pending "GET" -- deprecated, let's not support it
      it "POST" $ do
        Console.log "Posting 10,000 events"
        let events = bigEvent #
                      writeImpl >>> UndefinedEvent >>>
                      NEL.repeat >>> NEL.tail >>>
                      List.take 100 >>> toUnfoldable
        -- make sure it exists
        _ <- run $ postEventType undefinedET
        res <- sequence_ (List.take 100 $ List.repeat (
            (delay (200.0 # Milliseconds)) *>
            (run $ postEvents undefinedETN events)
          ))
        res `shouldEqual` unit
    describe "/event-types/{name}/partitions" $ do
      pending "GET"
    describe "/event-types/{name}/partitions/{partition}" $ do
      pending "GET"
    describe "/event-types/{name}/schemas" $ do
      pending "GET"
    describe "/event-types/{name}/schemas/{version}" $ do
      pending "GET"
    describe "/event-types/{name}/shifted-cursors" $ do
      pending "POST"
    describe "/event-types/{name}/timelines" $ do
      pending "GET"
      pending "POST"
    describe "/metrics" $ do
      pending "GET"
    describe "/registry/enrichment-strategies" $ do
      pending "GET"
    describe "/registry/partition-strategies" $ do
      pending "GET"
    describe "/settings/admins" $ do
      pending "GET"
      pending "POST"
    describe "/settings/blacklist" $ do
      pending "GET"
    describe "/settings/blacklist/{blacklist_type}/{name}" $ do
      pending "PUT"
      pending "DELETE"
    describe "/settings/features" $ do
      pending "GET"
      pending "POST"
    describe "/storages" $ do
      pending "GET"
      pending "POST"
    describe "/storages/default/{id}" $ do
      pending "PUT"
    describe "/storages/{id}" $ do
      pending "GET"
      pending "DELETE"
    describe "/subscriptions" $ do
      pending "GET"
      pending "POST"
    describe "/subscriptions/{subscription_id}" $ do
      pending "GET"
      pending "PUT"
      pending "DELETE"
    describe "/subscriptions/{subscription_id}/cursors" $ do
      pending "GET"
      pending "POST"
      pending "PATCH"
    describe "/subscriptions/{subscription_id}/events" $ do
      pending "GET"
      it "POST (local)" $ do
        let timeout = 10.0 # Seconds
        let minEvents = 4000

        let subs = Minimal.subscription (OwningApplication "cloud-juice") undefinedETN
        sub <- run $ postSubscription subs
        (void sub) `shouldEqual` (Right unit)
        let subId = fromMaybe (SubscriptionId "nein") (hush sub >>= _.id)
        events     <- liftEffect $ Ref.new 0
        characters <- liftEffect $ Ref.new 0
        startTime  <- liftEffect now
        res        <- run $ streamSubscriptionEvents (BufferSize $ 1024*1024) subId (Minimal.streamParameters 20 40) (handler events characters startTime)
        Console.log $ "Consuming for " <> show (unwrap timeout) <> " seconds"
        delay <<< convertDuration $ timeout
        Console.log "\nKilling in the name of the zablord"
        eventsProcessed <- liftEffect $ Ref.read events
        when (eventsProcessed < minEvents)
          (fail $ "Processed fewer than " <> show minEvents <> " events in " <> show (unwrap timeout) <> " seconds.")

    describe "/subscriptions/{subscription_id}/stats" $ do
      pending "GET"

handler :: ∀ a. WriteForeign a => Ref Int -> Ref Int -> Instant -> Array a -> Aff Unit
handler events characters startTime x = liftEffect $ do
  countBefore <- Ref.read events
  let asJson = writeJSON x
  Ref.modify_ (_ + String.length asJson) characters
  Ref.modify_ (_ + Array.length x) events
  count <- Ref.read events
  chars <- Ref.read characters
  let startMillis = unwrap (unInstant startTime)
  nowMillis <- now <#> unwrap <<< unInstant
  let speed = (toNumber count) / ((nowMillis - startMillis) / 1000.0)
  let megabytes = chars / (1024*1024)
  let text = "\r" <> show count <> " events processed (~" <> show megabytes <> " MB | " <> show (round speed) <> " events/sec)"
  void $ writeString stdout UTF8 text (pure unit)
  if count /= 10000
    then pure unit
    else (now >>= (\endTime -> Console.log ("\n" <> show ((unwrap (unInstant endTime)) - (unwrap (unInstant startTime))))))


bigEvent :: { korkenzieher :: String , items :: Array { korkenzieher :: Array String } }
bigEvent =
  { "korkenzieher": "milchhof brodowin"
  , "items":
    [ { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    , { "korkenzieher": ["milchhof", "brodowin"]}
    ]
  }
