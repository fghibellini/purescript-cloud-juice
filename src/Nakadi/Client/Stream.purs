module Nakadi.Client.Stream
  ( postStream
  , StreamReturn(..)
  , StreamResult(..)
  , StreamError
  , CommitError
  , CommitResult
  , BatchQueueItem(..)
  , InitialRequestError(..)
  )
  where

import Prelude

import Affjax as AX
import Control.Monad.Reader (ReaderT, runReaderT)
import Data.Array (length)
import Data.Either (Either(..), either)
import Data.Foldable (traverse_)
import Data.JSDate (now)
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.String as String
import Data.Variant (Variant)
import Effect (Effect)
import Effect.AVar as AVar
import Effect.Aff (Aff, Error, Milliseconds, launchAff_, message, runAff_)
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVarAff
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Exception (error, throwException)
import Effect.Ref as Ref
import Foreign (Foreign)
import Foreign.Object as Object
import Gzip.Gzip as Gzip
import Nakadi.Client.Internal (deserialiseProblem, jsonErr)
import Nakadi.Client.Types (NakadiResponse, Env)
import Nakadi.Errors (E400, E401, E403, E404, E409, E422, E_UNEXPECTED, e400, e401, e403, e404, e409, eUnexpected)
import Nakadi.Helpers (diffJSDate)
import Nakadi.Types (Event, Problem(..), StreamParameters, SubscriptionCursor, SubscriptionEventStreamBatch, SubscriptionId, SubscriptionStats, XNakadiStreamId(..))
import Node.Encoding (Encoding(..))
import Node.HTTP.Client as HTTP
import Node.Stream (Read, Stream, onData, onDataString, onEnd, onError, pipe)
import Node.Stream as Stream
import Node.Stream.Util (BufferSize, allocUnsafe, splitAtNewline)
import Simple.JSON (E)
import Simple.JSON as JSON

type StreamError = Variant
  ( unauthorized ∷ E401
  , badRequest ∷ E400
  , forbidden  ∷ E403
  , notFound   ∷ E404
  , conflict   ∷ E409
  , unexpected :: E_UNEXPECTED
  , ajaxError  :: AX.Error
  )

type CommitError = Variant
  ( unauthorized ∷ E401
  , forbidden ∷ E403
  , notFound ∷ E404
  , unprocessableEntity ∷ E422
  , unexpected :: E_UNEXPECTED
  , ajaxError :: AX.Error
  )

type StreamReturn = { result :: StreamResult, stats :: SubscriptionStats }

data StreamResult
  = FailedToStream StreamError
  | FailedToCommit { processingTime :: Milliseconds, commitError :: CommitError }
  | ErrorThrown Error
  | InitialRequestFailed InitialRequestError
  | InitialRequestStreamError Error
  | CloudJuiceInternalError Error
  | StreamClosed

data InitialRequestError
  = InitialRequestOnError Error
  | InitialRequestSendBodyError Error

type EventHandler = Array Event -> Aff Unit

type CommitResult =
  NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) Unit

data BatchQueueItem = Batch Foreign | EndOfStream StreamResult

postStream
  ∷ ∀ r
  . { resultVar ∷ AVar StreamResult
    , bufsize ∷ BufferSize
    , batchQueue :: AVar BatchQueueItem
    , batchConsumerLoopTerminated :: AVar (Either Error Unit)
    , subscriptionStats :: Ref.Ref SubscriptionStats
    }
  -> StreamParameters
  -> (SubscriptionId -> XNakadiStreamId -> Array SubscriptionCursor -> ReaderT (Env r) Aff CommitResult)
  -> SubscriptionId
  -> EventHandler
  -> Env r
  -> HTTP.Response
  -> Effect Unit
postStream postArgs@{ resultVar, bufsize, batchQueue, batchConsumerLoopTerminated } streamParams commitCursors subscriptionId eventHandler env response = do
  let _ = HTTP.statusMessage response
  let isGzipped = getHeader "Content-Encoding" response <#> String.contains (String.Pattern "gzip") # fromMaybe false
  let baseStream = HTTP.responseAsStream response
  Stream.onClose baseStream (void $ AVar.tryPut StreamClosed resultVar)
  stream <-
    if isGzipped
    then do
      gunzip <- Gzip.gunzip
      Stream.onError gunzip (const $ pure unit)
      pipe baseStream gunzip
    else
      pure baseStream

  let httpStatusCode = HTTP.statusCode response
  if httpStatusCode /= 200
    then do
      handleRequestErrors env httpStatusCode stream \streamReturn -> do
          void $ AVar.tryPut streamReturn resultVar
          void $ AVar.tryPut (Right unit) batchConsumerLoopTerminated
    else do
      -- Positive response, so we reset the backoff
      xStreamId <- XNakadiStreamId <$> getHeaderOrThrow "X-Nakadi-StreamId" response
      let commit = mkCommit xStreamId
      handleRequest postArgs streamParams stream commit eventHandler env
  where
    mkCommit xStreamId cursors =
      if cursors == mempty then do pure (Right unit)
      else
        commitCursors subscriptionId xStreamId cursors

handleRequest
  ∷ ∀ env r
  . { resultVar ∷ AVar StreamResult
    , bufsize ∷ BufferSize
    , batchQueue :: AVar BatchQueueItem
    , batchConsumerLoopTerminated :: AVar (Either Error Unit)
    , subscriptionStats :: Ref.Ref SubscriptionStats
    }
  -> StreamParameters
  -> Stream (read ∷ Read | r)
  -> (Array SubscriptionCursor -> ReaderT (Env env) Aff CommitResult)
  -> EventHandler
  -> Env env
  -> Effect Unit
handleRequest { resultVar, bufsize, batchQueue, batchConsumerLoopTerminated, subscriptionStats } streamParams resStream commit eventHandler env = do
  -- 1. the JSON batches are collected in a queue and processed by a loop
  --    of Aff operations
  -- 2. an AVar is used as a queue since the docs mention that on multiple puts it will behave like a FIFO sequence
  -- 3. the queue should not grow too big as it's limited by the configuration of the Nakadi consumer
  --    (you will only receive more batches once you commit the cursors)
  env.logWarn Nothing "[debug] entered handleRequest"
  buffer <- liftEffect $ allocUnsafe bufsize
  callback <- splitAtNewline buffer bufsize handleWorkerError enqueueBatch
  onData resStream callback
  onEnd resStream (terminateQueue StreamClosed)
  onError resStream
    ( \e -> do
        env.logWarn Nothing $ "Error in read stream " <> message e
        terminateQueue (InitialRequestStreamError e)
    )
  launchAff_ do -- listen for resultVar changes from upstream (i.e. from the caller of handleRequest)
    liftEffect $ env.logWarn Nothing "[debug] handleRequest waiting for resultVar"
    result <- AVarAff.read resultVar
    liftEffect $ env.logWarn Nothing "[debug] handleRequest resultVar handler"
    liftEffect $ terminateQueue result
  flip runAff_ batchConsumerLoop case _ of
    Right res -> do
      env.logWarn Nothing "[debug] handleRequest - batchConsumerLoop terminated successfully"
      void $ AVar.tryPut res resultVar
      void $ AVar.tryPut (Right unit) batchConsumerLoopTerminated
    Left e -> do
      env.logWarn Nothing "[debug] handleRequest - batchConsumerLoop terminated with an error"
      let err = error $ "Error in processing " <> show e
      void $ AVar.tryPut (ErrorThrown err) resultVar
      void $ AVar.tryPut (Left err) batchConsumerLoopTerminated
  where
    enqueueBatch :: Foreign -> Effect Unit
    enqueueBatch batch = do
      _ <- Ref.modify (\stats -> stats { receivedBatchCount = stats.receivedBatchCount + 1 }) subscriptionStats
      void $ AVar.put (Batch batch) batchQueue mempty

    terminateQueue :: StreamResult -> Effect Unit
    terminateQueue returnValue = void $ AVar.put (EndOfStream returnValue) batchQueue mempty

    handleWorkerError :: Error -> Effect Unit
    handleWorkerError e = do
        env.logWarn Nothing $ "Error in processing Nakadi response JSON lines: " <> message e
        terminateQueue (ErrorThrown e)

    batchConsumerLoop :: Aff StreamResult
    batchConsumerLoop = do
      queueHead <- AVarAff.read batchQueue -- we `read` instead of `take` so that the EndOfStream can stay in the queue
      case queueHead of
        EndOfStream res -> pure res
        Batch batch -> do
          _ <- AVarAff.take batchQueue
          handleBatch batch >>= case _ of
            Left res -> pure res
            Right _ -> batchConsumerLoop

    handleBatch :: Foreign -> Aff (Either StreamResult Unit)
    handleBatch batchJson = do
      t0 <- liftEffect now
      let parseFn = JSON.read ∷ Foreign -> E SubscriptionEventStreamBatch
      batch <- either jsonErr pure (parseFn batchJson)
      traverse_ eventHandler batch.events
      commitResult <- runReaderT (commit [ batch.cursor ]) env
      t1 <- liftEffect now
      case commitResult of
        Left err -> do
          let dt = diffJSDate t1 t0
          pure $ Left (FailedToCommit { processingTime: dt, commitError: err })
        Right other -> do
          _ <- liftEffect $ Ref.modify (\stats -> stats
            { committedBatchCount = stats.committedBatchCount + 1
            , committedEventCount = stats.committedEventCount + maybe 0 length batch.events
            , lastCommitTime = Just t1
            }) subscriptionStats
          pure $ Right unit


handleRequestErrors ∷ ∀ r s. Env s -> Int -> Stream (read ∷ Read | r) -> (StreamResult -> Effect Unit) -> Effect Unit
handleRequestErrors env httpStatus response cb = do
  env.logWarn Nothing "[debug] entered handleRequestErrors"
  buffer <- liftEffect $ Ref.new ""
  onDataString response UTF8 (\x -> Ref.modify_ (_ <> x) buffer)
  onEnd response
    $ runAff_ handleAffResult do
        str <- liftEffect $ Ref.read buffer
        liftEffect <<< Console.log $ "Nakadi responded with http status " <> show httpStatus <> " response body: " <> str -- TODO remove
        case deserialiseProblem httpStatus str of
          p@(NakadiErrorResponse { status: 400 }) -> pure $ e400 p
          p@(NakadiErrorResponse { status: 401 }) -> pure $ e401 p
          p@(NakadiErrorResponse { status: 403 }) -> pure $ e403 p
          p@(NakadiErrorResponse { status: 404 }) -> pure $ e404 p
          p@(NakadiErrorResponse { status: 409 }) -> pure $ e409 p
          p -> pure $ eUnexpected p
  where
    handleAffResult ∷ _ -> Effect _
    handleAffResult = case _ of
      Left e -> do
        let err = error $ "Error in processing non-200 response from Nakadi: " <> show e
        cb $ CloudJuiceInternalError err
      Right err -> do
        cb $ FailedToStream err

getHeader ∷ String -> HTTP.Response -> Maybe String
getHeader headerName response = do
  let responseHeaders = HTTP.responseHeaders response
  -- [WARN]: response headers are always lowercased
  let headerNameLower = String.toLower headerName
  Object.lookup headerNameLower responseHeaders

getHeaderOrThrow ∷ String -> HTTP.Response -> Effect String
getHeaderOrThrow headerName =
  maybe (throwException (error $ "Required header " <> headerName <> " is missing")) pure <<<
      getHeader headerName
