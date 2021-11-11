module Nakadi.Client
 ( getEventTypes
 , postEventType
 , getEventType
 , putEventType
 , deleteEventType
 , postEvents
 , postSubscription
 , streamSubscriptionEvents
 )
 where

import Prelude

import Affjax.RequestHeader (RequestHeader(..))
import Affjax.StatusCode (StatusCode(..))
import Control.Alt ((<|>))
import Control.Monad.Error.Class (class MonadError, class MonadThrow)
import Control.Monad.Reader (class MonadAsk, ask)
import Data.Bifunctor (lmap)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..), fromMaybe, isJust)
import Data.Newtype (unwrap)
import Data.Options ((:=))
import Data.String as String
import Data.Time.Duration (Milliseconds(..), Minutes(..))
import Data.Tuple (Tuple(..))
import Data.Variant (default, on)
import Effect (Effect)
import Effect.Aff (Aff, message)
import Effect.Aff.AVar as AVar
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Aff.Retry (RetryPolicyM, RetryStatus, capDelay, fullJitterBackoff, retrying)
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Exception (Error)
import Foreign.Object as Object
import Nakadi.Client.Internal (catchErrors, deleteRequest, deserialise, deserialiseProblem, deserialise_, formatErr, getRequest, postRequest, putRequest, readJson, request, unhandled)
import Nakadi.Client.Stream (CommitResult, StreamReturn(..), postStream)
import Nakadi.Client.Types (Env, NakadiResponse, LogWarnFn)
import Nakadi.Errors (E207, E400, E403, E404, E409(..), E422(..), E422Publish, _conflict, _unprocessableEntity, e207, e400, e401, e403, e404, e409, e422, e422Publish)
import Nakadi.Types (Cursor, CursorDistanceQuery, CursorDistanceResult, Event, EventType, EventTypeName(..), Partition, StreamParameters, Subscription, SubscriptionCursor, SubscriptionId(..), XNakadiStreamId(..))
import Node.Encoding (Encoding(..))
import Node.HTTP.Client (Request)
import Node.HTTP.Client as HTTP
import Node.Stream as Stream
import Node.Stream.Util (BufferSize, agent, allocUnsafe, newHttpKeepAliveAgent, newHttpsKeepAliveAgent)
import Simple.JSON (writeJSON)

getEventTypes
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => m (NakadiResponse () (Array EventType))
getEventTypes = do
  res <- getRequest "/event-types" >>= request >>= deserialise
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p -> unhandled p

postEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventType
  -> m (NakadiResponse (conflict ∷ E409, unprocessableEntity ∷ E422) Unit)
postEventType eventType = do
  res <- postRequest "/event-types" eventType >>= request >>= deserialise_
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 409 } -> pure $ lmap e409 res
    p @ { status: 422 } -> pure $ lmap e422 res
    p -> unhandled p

getEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> m (NakadiResponse (notFound ∷ E404) EventType)
getEventType (EventTypeName name) = do
  res <- getRequest ("/event-types/" <> name) >>= request >>= deserialise
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 404 } -> pure $ lmap e404 res
    p -> unhandled p

putEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> EventType
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) Unit)
putEventType (EventTypeName name) eventType = do
  res <- putRequest ("/event-types/" <> name) eventType >>= request >>= deserialise_
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 403 } -> pure $ lmap e403 res
    p @ { status: 404 } -> pure $ lmap e404 res
    p @ { status: 422 } -> pure $ lmap e422 res
    p -> unhandled p

deleteEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404) Unit)
deleteEventType (EventTypeName name) = do
  res <- deleteRequest ("/event-types/" <> name) >>= request  >>= deserialise_
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 403 } -> pure $ lmap e403 res
    p @ { status: 404 } -> pure $ lmap e404 res
    p -> unhandled p

getCursorDistances
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> (Array CursorDistanceQuery)
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) (Array CursorDistanceResult))
getCursorDistances (EventTypeName name) queries = do
  let path = "/event-types/" <> name <> "/cursor-distances"
  res <- postRequest path queries >>= request >>= deserialise
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 403 } -> pure $ lmap e403 res
    p @ { status: 404 } -> pure $ lmap e404 res
    p -> unhandled p

getCursorLag
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> (Array Cursor)
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) (Array Partition))
getCursorLag (EventTypeName name) cursors = do
  let path = "/event-types/" <> name <> "/cursors-lag"
  res <- postRequest path cursors >>= request >>= deserialise
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 403 } -> pure $ lmap e403 res
    p @ { status: 404 } -> pure $ lmap e404 res
    p -> unhandled p

postEvents
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> (Array Event)
  -> m (NakadiResponse (multiStatus ∷ E207, forbidden ∷ E403, notFound ∷ E404, unprocessableEntityPublish ∷ E422Publish) Unit)
postEvents (EventTypeName name) events = do
  let path = "/event-types/" <> name <> "/events"
  response <- postRequest path events >>= request
  case response of
    Right { body, status: StatusCode statusCode } -> do
      res1 <- case statusCode of
        code | code == 422 || code == 207 -> map Just <$> readJson body
        code | code # between 200 299 -> (pure <<< pure) Nothing
        _ -> deserialiseProblem body
      res2 <- res1 # catchErrors case _ of -- is this all correct?
        p @ { status: 403 } -> pure $ lmap e403 res1
        p @ { status: 404 } -> pure $ lmap e404 res1
        p -> unhandled p
      pure $ res2 >>= case _ of
        Just batchProblem ->
          if statusCode == 207
          then Left $ e207 batchProblem -- this is treated as an error
          else Left $ e422Publish batchProblem
        Nothing -> Right unit
    Left error ->
      formatErr error

postSubscription
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => Subscription
  -> m (NakadiResponse (badRequest ∷ E400, unprocessableEntity ∷ E422) Subscription)
postSubscription subscription = do
  let path = "/subscriptions"
  res <- postRequest path subscription >>= request >>= deserialise
  res # catchErrors case _ of
    p @ { status: 400 } -> pure $ lmap e400 res
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 422 } -> pure $ lmap e422 res
    p -> unhandled p

commitCursors
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => SubscriptionId
  -> XNakadiStreamId
  -> Array SubscriptionCursor
  -> m CommitResult
commitCursors (SubscriptionId id) (XNakadiStreamId header) items = do
  let path = "/subscriptions/" <> id <> "/cursors"
  standardRequest <- postRequest path { items }
  let req = standardRequest { headers = standardRequest.headers <> [RequestHeader "X-Nakadi-StreamId" header] }
  res <- request req >>= deserialise_
  res # catchErrors case _ of
    p @ { status: 401 } -> pure $ lmap e401 res
    p @ { status: 403 } -> pure $ lmap e403 res
    p @ { status: 422 } -> pure $ lmap e422 res
    p -> unhandled p


foreign import removeRequestTimeout ∷ Request -> Effect Unit

streamSubscriptionEvents
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadError Error m
  => MonadAff m
  => BufferSize
  -> SubscriptionId
  -> StreamParameters
  -> (Array Event -> Aff Unit)
  -> m StreamReturn
streamSubscriptionEvents bufsize sid@(SubscriptionId subId) streamParameters eventHandler = do
  env    <- ask
  buffer <- liftEffect $ allocUnsafe bufsize

  let
    listen postArgs = do
      token <- env.token
      let headers' =
            [ Tuple "X-Flow-ID" (unwrap env.flowId)
            , Tuple "Authorization" token
            , Tuple "Content-Type"    "application/json"
            , Tuple "Accept"          "application/json"
            -- , Tuple "Accept-Encoding" "gzip"
            ]
      let headers = Object.fromFoldable headers'
      let https = String.stripPrefix (String.Pattern "https://") env.baseUrl
      let http  = String.stripPrefix (String.Pattern "http://") env.baseUrl
      keepAliveAgent <- liftEffect $
        if isJust http then newHttpKeepAliveAgent else newHttpsKeepAliveAgent
      let hostname = fromMaybe env.baseUrl (https <|> http)
      let protocol = if isJust http then "http:" else "https:"
      let options = HTTP.protocol := protocol
                  <> HTTP.hostname := hostname
                  <> HTTP.port     := env.port
                  <> HTTP.headers  := HTTP.RequestHeaders headers
                  <> HTTP.method   := "POST"
                  <> HTTP.path     := ("/subscriptions/" <> subId <> "/events")
                  <> agent         := keepAliveAgent

      let requestCallback = postStream postArgs streamParameters commitCursors sid eventHandler env

      req <- HTTP.request options requestCallback
      removeRequestTimeout req
      let writable = HTTP.requestAsStream req
      let body = writeJSON streamParameters
      Stream.onError writable (\e -> Console.log $ "Error!!!" <> message e)
      let endStream = Stream.end writable (pure unit)
      _ <- Stream.writeString writable UTF8 body endStream
      pure unit

    go ∷ m StreamReturn
    go = do
        resultVar <- liftAff AVar.empty
        batchQueue <- liftAff AVar.empty
        let postArgs = { resultVar
                       , buffer
                       , bufsize
                       , batchQueue
                       }
        liftEffect $ listen postArgs
        liftAff $ AVar.take resultVar

    retryPolicy ∷ RetryPolicyM m
    retryPolicy = capDelay (3.0 # Minutes) $ fullJitterBackoff (200.0 # Milliseconds)
    retryCheck ∷ LogWarnFn -> RetryStatus -> StreamReturn -> m Boolean
    retryCheck logWarn _ res =
      liftEffect
        $ case res of
            StreamClosed -> logWarn Nothing "Stream closed by Nakadi" $> false
            FailedToStream err ->
              err
                # on _conflict (\(E409 p) -> logWarn (Just p) "Failed to start streaming." $> true)
                    (default (pure true))
            ErrorThrown err -> logWarn Nothing "Exception in consumer" $> true
            FailedToCommit err ->
              err
                # on _unprocessableEntity (\(E422 p) -> logWarn (Just p) "Failed to commit cursor." $> true)
                    (default (pure true))
  retrying retryPolicy (retryCheck env.logWarn) (const go)
