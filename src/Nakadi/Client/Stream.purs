module Nakadi.Client.Stream
  ( postStream
  , StreamReturn(..)
  , StreamError
  , CommitError
  , CommitResult
  , BatchQueueItem(..)
  )
  where

import Prelude

import Control.Monad.Reader (ReaderT, runReaderT)
import Data.Either (Either(..), either)
import Data.Foldable (traverse_)
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.String as String
import Data.Variant (Variant)
import Effect (Effect)
import Effect.AVar as AVar
import Effect.Aff (Aff, Error, launchAff_, message, runAff_)
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVarAff
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Exception (error, throwException)
import Effect.Ref as Ref
import Foreign (Foreign)
import Foreign.Object as Object
import Gzip.Gzip as Gzip
import Nakadi.Client.Internal (jsonErr, unhandled)
import Nakadi.Client.Types (NakadiResponse, Env)
import Nakadi.Errors (E400, E401, E403, E404, E409, E422, e400, e401, e403, e404, e409)
import Nakadi.Types (Event, StreamParameters, SubscriptionCursor, SubscriptionId, XNakadiStreamId(..), SubscriptionEventStreamBatch)
import Node.Encoding (Encoding(..))
import Node.HTTP.Client as HTTP
import Node.Stream (Read, Stream, onData, onDataString, onEnd, onError, pipe)
import Node.Stream as Stream
import Node.Stream.Util (BufferSize, allocUnsafe, splitAtNewline)
import Simple.JSON (E, readJSON)
import Simple.JSON as JSON

type StreamError = Variant
  ( unauthorized ∷ E401
  , badRequest ∷ E400
  , forbidden  ∷ E403
  , notFound   ∷ E404
  , conflict   ∷ E409
  )

type CommitError = Variant
  ( unauthorized ∷ E401
  , forbidden ∷ E403
  , notFound ∷ E404
  , unprocessableEntity ∷ E422
  )

data StreamReturn
  = FailedToStream StreamError
  | FailedToCommit CommitError
  | ErrorThrown Error
  | StreamClosed

type EventHandler = Array Event -> Aff Unit

type CommitResult =
  NakadiResponse (forbidden ∷ E403, notFound ∷ E404, unprocessableEntity ∷ E422) Unit

data BatchQueueItem = Batch Foreign | EndOfStream StreamReturn

postStream
  ∷ ∀ r
  . { resultVar ∷ AVar StreamReturn
    , bufsize ∷ BufferSize
    , batchQueue :: AVar BatchQueueItem
    , batchConsumerLoopTerminated :: AVar Unit
    }
  -> StreamParameters
  -> (SubscriptionId -> XNakadiStreamId -> Array SubscriptionCursor -> ReaderT (Env r) Aff CommitResult)
  -> SubscriptionId
  -> EventHandler
  -> Env r
  -> HTTP.Response
  -> Effect Unit
postStream { resultVar, bufsize, batchQueue, batchConsumerLoopTerminated } streamParams commitCursors subscriptionId eventHandler env response = do
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
    then handleRequestErrors httpStatusCode resultVar stream
    else do
      -- Positive response, so we reset the backoff
      xStreamId <- XNakadiStreamId <$> getHeaderOrThrow "X-Nakadi-StreamId" response
      let commit = mkCommit xStreamId
      handleRequest { resultVar, bufsize, batchQueue, batchConsumerLoopTerminated } streamParams stream commit eventHandler env
  where
    mkCommit xStreamId cursors =
      if cursors == mempty then do pure (Right unit)
      else
        commitCursors subscriptionId xStreamId cursors

handlePutAVarError ∷ Either Error Unit -> Effect Unit
handlePutAVarError = case _ of
    Left e -> do
      liftEffect $ throwException (error $ "Error setting AVar " <> show e)
    Right a -> pure unit

handleRequest
  ∷ ∀ env r
  . { resultVar ∷ AVar StreamReturn
    , bufsize ∷ BufferSize
    , batchQueue :: AVar BatchQueueItem
    , batchConsumerLoopTerminated :: AVar Unit
    }
  -> StreamParameters
  -> Stream (read ∷ Read | r)
  -> (Array SubscriptionCursor -> ReaderT (Env env) Aff CommitResult)
  -> EventHandler
  -> Env env
  -> Effect Unit
handleRequest { resultVar, bufsize, batchQueue, batchConsumerLoopTerminated } streamParams resStream commit eventHandler env = do
  -- 1. the JSON batches are collected in a queue and processed by a loop
  --    of Aff operations
  -- 2. an AVar is used as a queue since the docs mention that on multiple puts it will behave like a FIFO sequence
  -- 3. the queue should not grow too big as it's limited by the configuration of the Nakadi consumer
  --    (you will only receive more batches once you commit the cursors)
  buffer <- liftEffect $ allocUnsafe bufsize
  callback <- splitAtNewline buffer bufsize handleWorkerError enqueueBatch
  onData resStream callback
  onEnd resStream (terminateQueue StreamClosed)
  onError resStream
    ( \e -> do
        env.logWarn Nothing $ "Error in read stream " <> message e
        terminateQueue (ErrorThrown e)
    )
  launchAff_ do -- listen for resultVar changes from upstream (i.e. from the caller of handleRequest)
    result <- AVarAff.read resultVar
    liftEffect $ terminateQueue result
  flip runAff_ batchConsumerLoop case _ of
    Right a -> pure unit
    Left e -> do
      let err = error $ "Error in processing " <> show e
      void $ AVar.tryPut (ErrorThrown err) resultVar
      void $ AVar.tryPut unit batchConsumerLoopTerminated
  where
    enqueueBatch :: Foreign -> Effect Unit
    enqueueBatch batch = void $ AVar.put (Batch batch) batchQueue mempty

    terminateQueue :: StreamReturn -> Effect Unit
    terminateQueue returnValue = void $ AVar.put (EndOfStream returnValue) batchQueue mempty

    handleWorkerError :: Error -> Effect Unit
    handleWorkerError e = do
        env.logWarn Nothing $ "Error in processing Nakadi response JSON lines: " <> message e
        terminateQueue (ErrorThrown e)

    batchConsumerLoop :: Aff Unit
    batchConsumerLoop = do
      queueHead <- AVarAff.read batchQueue -- we `read` instead of `take` so that the EndOfStream can stay in the queue
      case queueHead of
        EndOfStream res -> do
          void $ AVarAff.tryPut res resultVar
          void $ AVarAff.tryPut unit batchConsumerLoopTerminated
        Batch batch -> do
          _ <- AVarAff.take batchQueue
          _ <- handleBatch batch
          batchConsumerLoop

    handleBatch batchJson = do
      let parseFn = JSON.read ∷ Foreign -> E SubscriptionEventStreamBatch
      batch <- either jsonErr pure (parseFn batchJson)
      traverse_ eventHandler batch.events
      commitResult <- runReaderT (commit [ batch.cursor ]) env
      case commitResult of
        Left err -> void $ liftEffect $ AVar.tryPut (FailedToCommit err) resultVar
        Right other -> pure unit


handleRequestErrors ∷ ∀ r. Int -> AVar StreamReturn → Stream (read ∷ Read | r) -> Effect Unit
handleRequestErrors httpStatus resultVar response = do
  buffer <- liftEffect $ Ref.new ""
  onDataString response UTF8 (\x -> Ref.modify_ (_ <> x) buffer)
  onEnd response
    $ runAff_ handleAffResult do
        str <- liftEffect $ Ref.read buffer
        liftEffect <<< Console.log $ "Nakadi responded with http status " <> show httpStatus <> " response body: " <> str
        res <- (readJSON >>> either jsonErr pure) str
        case res of
          p@{ status: 400 } -> pure $ e400 res
          p@{ status: 401 } -> pure $ e401 res
          p@{ status: 403 } -> pure $ e403 res
          p@{ status: 404 } -> pure $ e404 res
          p@{ status: 409 } -> pure $ e409 res
          p -> unhandled p
  where
    handleAffResult ∷ _ -> Effect _
    handleAffResult = case _ of
      Left e -> do
        let err = error $ "Error in processing non-200 response from Nakadi: " <> show e
        void $ AVar.tryPut (ErrorThrown err) resultVar
      Right err -> void $ AVar.tryPut (FailedToStream err) resultVar

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
