module Nakadi.Client
 ( getEventTypes
 , postEventType
 , getEventType
 , putEventType
 , deleteEventType
 , postEvents
 , postSubscription
 , streamSubscriptionEvents
 , streamSubscriptionEventsRetrying
 )
 where

import Prelude

import Affjax.RequestHeader (RequestHeader(..))
import Affjax.StatusCode (StatusCode(..))
import Control.Alt ((<|>))
import Control.Monad.Error.Class (class MonadError, class MonadThrow)
import Control.Monad.Reader (class MonadAsk, ask)
import Control.Parallel (parOneOf)
import Data.Bifunctor (lmap)
import Data.Either (Either(..))
import Data.JSDate (now)
import Data.Maybe (Maybe(..), fromMaybe, isJust)
import Data.Newtype (unwrap)
import Data.Options ((:=))
import Data.String as String
import Data.Time.Duration (Milliseconds(..), Seconds(..))
import Data.Tuple (Tuple(..))
import Data.Variant (default, match, on)
import Effect (Effect)
import Effect.AVar as AV
import Effect.Aff (Aff, makeAff, nonCanceler, runAff_)
import Effect.Aff.AVar as AVar
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Aff.Retry (RetryPolicyM, RetryStatus, capDelay, fullJitterBackoff, retrying)
import Effect.Class (liftEffect)
import Effect.Exception (Error)
import Effect.Ref as Ref
import Foreign.Object as Object
import Nakadi.Client.Internal (deleteRequest, deserialiseProblem, getRequest, postRequest, processResponseNoBody, processResponseWithBody, putRequest, request)
import Nakadi.Client.Stream (CommitResult, InitialRequestError(..), StreamResult(..), StreamReturn, postStream)
import Nakadi.Client.Types (Env, NakadiResponse, LogWarnFn, SpanCtx)
import Nakadi.Errors (E207, E400, E403, E404, E409(..), E422(..), E422Publish, _conflict, _unprocessableEntity, e207, e401, e403, e404, e409, e422, e422Publish, eAjaxError, eUnexpected)
import Nakadi.Types (Cursor, CursorDistanceQuery, CursorDistanceResult, Event, EventType, EventTypeName(..), Partition, Problem(..), StreamParameters, Subscription, SubscriptionCursor, SubscriptionId(..), XNakadiStreamId(..), emptySubscriptionStats, problemStatus)
import Node.Encoding (Encoding(..))
import Node.HTTP.Client (Request)
import Node.HTTP.Client as HTTP
import Node.Stream as Stream
import Node.Stream.Util (BufferSize, agent, newHttpAgent, newHttpsAgent, destroyAgent)
import Simple.JSON (class WriteForeign, readJSON, writeJSON)

getEventTypes
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => m (NakadiResponse () (Array EventType))
getEventTypes = do
  res <- getRequest "/event-types" >>= request >>= processResponseWithBody
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          _ -> eUnexpected p
      }

postEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => Maybe SpanCtx
  -> EventType
  -> m (NakadiResponse (conflict ∷ E409, unprocessableEntity ∷ E422) Unit)
postEventType spanCtx eventType = do
  res <- processResponseNoBody <$> (postRequest "/event-types" spanCtx eventType >>= request)
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          409 -> e409 p
          422 -> e422 p
          _ -> eUnexpected p
      }

getEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> m (NakadiResponse (notFound ∷ E404) EventType)
getEventType (EventTypeName name) = do
  res <- getRequest ("/event-types/" <> name) >>= request >>= processResponseWithBody
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          404 -> e404 p
          _ -> eUnexpected p
      }


putEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => Maybe SpanCtx
  -> EventTypeName
  -> EventType
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) Unit)
putEventType spanCtx (EventTypeName name) eventType = do
  res <- processResponseNoBody <$> (putRequest ("/event-types/" <> name) spanCtx eventType >>= request)
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          403 -> e403 p
          404 -> e404 p
          422 -> e422 p
          _ -> eUnexpected p
      }

-- TODO
-- wrapErrorCodes
--   :: forall r1 r2
--   . Union r1 (ajaxError :: AX.Error) r2
--   => (Int -> Problem -> Variant r1)
--   -> (Variant (ajaxError :: AX.Error, httpError :: Problem))
--   -> Variant r2
-- wrapErrorCodes fn res = match
--     { ajaxError: \err -> unsafeCoerce $ eAjaxError err -- ((eAjaxError err) :: (Variant (ajaxError :: AX.Error | r1)))
--     , httpError: \p ->
--       let statusCode = problemStatus p
--       in expand $ fn statusCode p
--     } res

deleteEventType
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => EventTypeName
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404) Unit)
deleteEventType (EventTypeName name) = do
  res <- processResponseNoBody <$> (deleteRequest ("/event-types/" <> name) >>= request)
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          403 -> e403 p
          404 -> e404 p
          _ -> eUnexpected p
      }


getCursorDistances
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => Maybe SpanCtx
  -> EventTypeName
  -> (Array CursorDistanceQuery)
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) (Array CursorDistanceResult))
getCursorDistances spanCtx (EventTypeName name) queries = do
  let path = "/event-types/" <> name <> "/cursor-distances"
  res <- postRequest path spanCtx queries >>= request >>= processResponseWithBody
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          403 -> e403 p
          404 -> e404 p
          _ -> eUnexpected p
      }

getCursorLag
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => Maybe SpanCtx
  -> EventTypeName
  -> (Array Cursor)
  -> m (NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) (Array Partition))
getCursorLag spanCtx (EventTypeName name) cursors = do
  let path = "/event-types/" <> name <> "/cursors-lag"
  res <- postRequest path spanCtx cursors >>= request >>= processResponseWithBody
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          403 -> e403 p
          404 -> e404 p
          _ -> eUnexpected p
      }


postEvents
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadAff m
  => Maybe SpanCtx
  -> EventTypeName
  -> (Array Event)
  -> m (NakadiResponse (multiStatus ∷ E207, forbidden ∷ E403, notFound ∷ E404, unprocessableEntityPublish ∷ E422Publish) Unit)
postEvents spanCtx (EventTypeName name) events = do
  let path = "/event-types/" <> name <> "/events"
  response <- postRequest path spanCtx events >>= request
  case response of
    Right { body, status: StatusCode statusCode } -> do
      case statusCode of
        code | code == 207 -> case readJSON body of
          Right x -> pure $ Left $ e207 x
          Left err -> pure $ Left $ eUnexpected $ HttpErrorResponse { status: code, body } -- response probably not from Nakadi
        code | code == 422 -> case readJSON body of
          Right x -> pure $ Left $ e422Publish x
          Left err -> pure $ Left $ eUnexpected $ HttpErrorResponse { status: code, body } -- response probably not from Nakadi
        code | code # between 200 299 -> pure $ Right unit
        code -> pure $ Left $
          let problem = deserialiseProblem statusCode body
          in case code of
            403 -> e403 problem
            404 -> e404 problem
            _ -> eUnexpected problem
    Left error ->
      pure $ Left $ eAjaxError error

postSubscription
  ∷ ∀ r m
   . MonadAsk (Env r) m
  => MonadThrow Error m
  => MonadAff m
  => Maybe SpanCtx
  -> Subscription
  -> m (NakadiResponse (badRequest ∷ E400, unprocessableEntity ∷ E422) Subscription)
postSubscription spanCtx subscription = do
  let path = "/subscriptions"
  res <- postRequest path spanCtx subscription >>= request >>= processResponseWithBody
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          400 -> e401 p
          401 -> e401 p
          422 -> e422 p
          _ -> eUnexpected p
      }

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
  let spanCtx = Nothing
  standardRequest <- postRequest path spanCtx { items }
  let req = standardRequest { headers = standardRequest.headers <> [RequestHeader "X-Nakadi-StreamId" header] }
  res <- processResponseNoBody <$> request req
  pure $ lmap mapErrorVariant res
  where
    mapErrorVariant = match
      { ajaxError: \err -> eAjaxError err
      , httpError: \p ->
        let statusCode = problemStatus p
        in case statusCode of
          401 -> e401 p
          403 -> e403 p
          422 -> e422 p
          _ -> eUnexpected p
      }


foreign import setRequestTimeout ∷ Number -> Request -> Effect Unit
foreign import requestOnError ∷ Request -> (Error -> Effect Unit) -> Effect Unit

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
  subscriptionTime <- liftEffect now
  subscriptionStats <- liftEffect $ Ref.new (emptySubscriptionStats subscriptionTime)
  let
    listen postArgs@{ resultVar, batchConsumerLoopInitiated, batchConsumerLoopTerminated } = do
      let
        terminateConsumptionWithError err = do
          void $ AV.tryPut err resultVar
          unlessM (Ref.read batchConsumerLoopInitiated) do
            void $ AV.tryPut (Right unit) batchConsumerLoopTerminated
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
      requestAgent <- liftEffect $ case http of
          Just _ -> newHttpAgent
          Nothing ->  newHttpsAgent
      let hostname = fromMaybe env.baseUrl (https <|> http)
      let protocol = if isJust http then "http:" else "https:"
      let options = HTTP.protocol := protocol
                  <> HTTP.hostname := hostname
                  <> HTTP.port     := env.port
                  <> HTTP.headers  := HTTP.RequestHeaders headers
                  <> HTTP.method   := "POST"
                  <> HTTP.path     := ("/subscriptions/" <> subId <> "/events")
                  <> agent         := requestAgent

      let requestCallback = postStream postArgs streamParameters commitCursors sid eventHandler env
      req <- HTTP.request options requestCallback
      setRequestTimeout (unwrap env.timeout) req
      requestOnError req \err -> do
          env.logWarn Nothing "[debug] streamSubscriptionEvents request 'error' handler invoked"
          terminateConsumptionWithError (InitialRequestFailed $ InitialRequestOnError err)
      runAffAndPropagateError terminateConsumptionWithError do
        sendBodyAndEnd req streamParameters
      pure requestAgent

    -- run an Aff asynchronously and if it terminates with an error write it to the AVar
    runAffAndPropagateError :: forall a. (StreamResult -> Effect Unit) -> Aff a -> Effect Unit
    runAffAndPropagateError errorCallback aff =
      flip runAff_ aff \x -> case x of
        Left err -> do
          env.logWarn Nothing "[debug] sendBodyAndEnd failed with an error"
          errorCallback (InitialRequestFailed $ InitialRequestSendBodyError err)
        Right _ -> do
          env.logWarn Nothing "[debug] sendBodyAndEnd completed successfully"
          pure unit

    sendBodyAndEnd :: forall a. WriteForeign a => Request -> a -> Aff Unit
    sendBodyAndEnd req body = do
      let
        writable = HTTP.requestAsStream req
        rawBody = writeJSON body
      void $ parOneOf
        [ Left <$> makeAff \cb -> Stream.onError writable (\e -> cb $ Left e) $> nonCanceler
        , Right <$> do
            makeAff \cb -> Stream.writeString writable UTF8 rawBody (cb $ Right unit) $> nonCanceler
            makeAff \cb -> Stream.end writable (cb $ Right unit) $> nonCanceler
        ]

  -- TODO
  -- we should get rid of all these AVars and instead operate on `Aff`s
  resultVar <- liftAff AVar.empty
  batchQueue <- liftAff AVar.empty
  batchConsumerLoopInitiated <- liftEffect $ Ref.new false
  batchConsumerLoopTerminated <- liftAff AVar.empty
  let postArgs = { resultVar
                  , bufsize
                  , batchQueue
                  , batchConsumerLoopInitiated
                  , batchConsumerLoopTerminated
                  , subscriptionStats
                  }
  -- `listen` is an `Effect` that will return immediately (it just sets up the event handlers)
  -- We use the AVar `resultVar` to signal termination of the Nakadi consumption.
  -- Even once `resultVar`` is populated we can still have a running batch handler or batches
  -- queued in `batchQueue`, so `batchConsumerLoopTerminated` is an additional signal that we wait for before returning.
  liftEffect $ env.logWarn Nothing "[debug] streamSubscriptionEvents: invoking listen"
  requestAgent <- liftEffect $ listen postArgs
  liftEffect $ env.logWarn Nothing "[debug] streamSubscriptionEvents: waiting for resultVar"
  res <- liftAff $ AVar.read resultVar
  liftEffect $ env.logWarn Nothing "[debug] streamSubscriptionEvents: waiting for batchConsumerLoopTerminated"
  consumerRes <- liftAff $ AVar.read batchConsumerLoopTerminated
  -- without this the server takes a loooong time to terminate (which is painful in the tests) see https://nodejs.org/api/http.html#agentdestroy
  liftEffect $ env.logWarn Nothing "[debug] streamSubscriptionEvents: invoking destroyAgent"
  liftEffect $ destroyAgent requestAgent
  liftEffect $ env.logWarn Nothing "[debug] streamSubscriptionEvents: destroyAgent returned"
  -- the batches are queued up, so even if the connection ends up being terminated by Nakadi
  -- we still want to return an error if any of the queued batches throws an error
  stats <- liftEffect $ Ref.read subscriptionStats
  pure $ { stats, result: _ } $ case consumerRes of
    Left err -> ErrorThrown err
    Right _ -> res

streamSubscriptionEventsRetrying
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
streamSubscriptionEventsRetrying bufsize sid streamParameters eventHandler = do
  env    <- ask
  let
    retryCheck ∷ LogWarnFn -> RetryStatus -> StreamReturn -> m Boolean
    retryCheck logWarn _ res =
      liftEffect
        $ case res.result of
            StreamClosed -> logWarn Nothing "Stream closed by Nakadi" $> false
            FailedToStream err ->
              err
                # on _conflict (\(E409 p) -> logWarn (Just p) "Failed to start streaming." $> true)
                    (default (pure true))
            InitialRequestFailed err -> logWarn Nothing "InitialRequestFailed" $> true
            InitialRequestStreamError err -> logWarn Nothing "InitialRequestStreamError" $> true
            CloudJuiceInternalError err -> logWarn Nothing "CloudJuiceInternalError" $> true
            ErrorThrown err -> logWarn Nothing "Exception in consumer" $> true
            FailedToCommit { commitError } ->
              commitError
                # on _unprocessableEntity (\(E422 p) -> logWarn (Just p) "Failed to commit cursor." $> true)
                    (default (pure true))
    retryPolicy ∷ RetryPolicyM m
    retryPolicy = capDelay (3.0 # Seconds) $ fullJitterBackoff (200.0 # Milliseconds)
  retrying retryPolicy (retryCheck env.logWarn) (\_ -> streamSubscriptionEvents bufsize sid streamParameters eventHandler)
