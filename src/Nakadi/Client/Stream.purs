module Nakadi.Client.Stream
  ( postStream
  , StreamReturn(..)
  , StreamError
  , CommitError
  , CommitResult
  )
  where

import Prelude


import Control.Monad.Reader (ReaderT, runReaderT)
import Data.Bifunctor (lmap)
import Data.DateTime.Instant (Instant)
import Data.Either (Either(..), either)
import Data.Maybe (Maybe, fromMaybe, maybe)
import Data.String as String
import Data.Traversable (traverse_)
import Data.Variant (Variant)
import Effect (Effect)
import Effect.Aff (Aff, Error, forkAff, launchAff_, runAff_)
import Effect.Aff.AVar (AVar)
import Effect.Aff.AVar as AVar
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Effect.Exception (error, throwException)
import Effect.Now (now)
import Effect.Ref (Ref)
import Effect.Ref as Ref
import Foreign.Object as Object
import Gzip.Gzip as Gzip
import Nakadi.Client.Internal (catchErrors, jsonErr, unhandled)
import Nakadi.Client.Types (NakadiResponse, Env)
import Nakadi.Errors (E400, E401, E403, E404, E409, E422, e400, e401, e403, e404, e409)
import Nakadi.Types (Event, StreamParameters, SubscriptionCursor, SubscriptionId, XNakadiStreamId(..), SubscriptionEventStreamBatch)
import Node.Encoding (Encoding(..))
import Node.HTTP.Client as HTTP
import Node.Stream (Duplex, Read, Readable, Stream, Writable, onDataString, onEnd, pipe)
import Node.Stream as Stream
import Node.Stream.Util (splitWithFn, writable)
import Simple.JSON (E, readJSON)

type StreamError = Variant
  ( unauthorized :: E401
  , badRequest :: E400
  , forbidden  :: E403
  , notFound   :: E404
  , conflict   :: E409
  )

type CommitError = Variant
  ( unauthorized :: E401
  , forbidden :: E403
  , notFound :: E404
  , unprocessableEntity :: E422
  )

data StreamReturn
  = FailedToStream StreamError
  | FailedToCommit CommitError
  | StreamClosed

type EventHandler = Array Event -> Aff Unit

type CommitResult =
  NakadiResponse (forbidden :: E403, notFound :: E404, unprocessableEntity :: E422) Unit

postStream
  :: forall r
   . { resultVar :: AVar StreamReturn
     , batchesVar :: AVar (Array SubscriptionEventStreamBatch)
     , onStreamEstablished :: Effect Unit
     }
  -> StreamParameters
  -> (SubscriptionId -> XNakadiStreamId -> Array SubscriptionCursor -> ReaderT (Env r) Aff CommitResult)
  -> SubscriptionId
  -> EventHandler
  -> Env r
  -> HTTP.Response
  -> Effect Unit
postStream { resultVar, batchesVar, onStreamEstablished } streamParams commitCursors subscriptionId eventHandler env response = do
  let _ = HTTP.statusMessage response
  let isGzipped = getHeader "Content-Encoding" response <#> String.contains (String.Pattern "gzip") # fromMaybe false
  let baseStream = HTTP.responseAsStream response
  Stream.onClose baseStream (launchAff_ $ AVar.put StreamClosed resultVar)
  stream <-
    if isGzipped
    then do
      gunzip <- Gzip.gunzip
      Stream.onError gunzip (const $ pure unit)
      pipe baseStream gunzip
    else
      pure baseStream

  if HTTP.statusCode response /= 200
    then handleRequestErrors resultVar stream
    else do
      -- Positive response, so we reset the backoff
      onStreamEstablished
      xStreamId <- XNakadiStreamId <$> getHeaderOrThrow "X-Nakadi-StreamId" response
      start <- now
      lastCommit <- Ref.new start
      let commit = mkCommit xStreamId
      handleRequest { resultVar, batchesVar, lastCommit } streamParams stream commit eventHandler env
  where
  mkCommit xStreamId cursors =
    if cursors == mempty
    then do pure (Right unit)
    else commitCursors subscriptionId xStreamId cursors

handleUnhandledError :: Either Error Unit -> Effect Unit
handleUnhandledError = case _ of
    Left e -> do
      liftEffect $ throwException (error $ "Error in processing " <> show e)
    Right a -> pure unit

handlePutAVarError :: Either Error Unit -> Effect Unit
handlePutAVarError = case _ of
    Left e -> do
      liftEffect $ throwException (error $ "Error setting AVar " <> show e)
    Right a -> pure unit

pipeBoy :: forall b a. Effect (Readable a) -> Effect (Writable b) -> Effect (Writable b)
pipeBoy ma mb = do
  a <- ma
  b <- mb
  Stream.pipe a b

infixl 7 pipeBoy as |>

handleRequest
  :: forall env r
   . { resultVar :: AVar StreamReturn
     , batchesVar :: AVar (Array SubscriptionEventStreamBatch)
     , lastCommit :: Ref Instant
     }
  -> StreamParameters
  -> Stream (read ∷ Read | r)
  -> (Array SubscriptionCursor -> ReaderT (Env env) Aff CommitResult)
  -> EventHandler
  -> Env env
  -> Effect Unit
handleRequest { resultVar, batchesVar, lastCommit } streamParams reqStream commit eventHandler env = do
  let writer = writable writeBoy
  let splitFn = readJSON :: String -> E SubscriptionEventStreamBatch
  _    <- pure reqStream |> splitWithFn splitFn |> writer
  pure unit
  where
    writeBoy (batchesE :: E SubscriptionEventStreamBatch) _ done = runAff_ handleUnhandledError $ do
        batch <- either jsonErr pure batchesE
        eventHandler `traverse_` batch.events
        let commitBoy = do
              commitResult <- runReaderT (commit [batch.cursor]) env
              case commitResult of
                Left err ->
                  AVar.put (FailedToCommit err) resultVar
                Right other -> do
                  pure unit
        _ <- forkAff commitBoy
        liftEffect $ done

handleRequestErrors ∷ ∀ r. AVar StreamReturn → Stream (read ∷ Read | r) -> Effect Unit
handleRequestErrors resultVar response = do
  buffer <- liftEffect $ Ref.new ""
  onDataString response UTF8 (\x -> Ref.modify_ (_ <> x) buffer)
  onEnd response $ runAff_ throwError do
    str <- liftEffect $ Ref.read buffer
    liftEffect <<< Console.log $ "Result: " <> str
    res <- (readJSON >>> either jsonErr (pure <<< Left)) str
    result <- res # catchErrors case _ of
      p @ { status: 400 } -> pure $ lmap e400 res
      p @ { status: 401 } -> pure $ lmap e401 res
      p @ { status: 403 } -> pure $ lmap e403 res
      p @ { status: 404 } -> pure $ lmap e404 res
      p @ { status: 409 } -> pure $ lmap e409 res
      p -> unhandled p
    case result of
      Left err -> AVar.put (FailedToStream err) resultVar
      Right _ -> pure unit
  where
  throwError = case _ of
    Left e -> do
      liftEffect $ throwException (error $ "Error in processing " <> show e)
    Right a -> pure unit

getHeader :: String -> HTTP.Response -> Maybe String
getHeader headerName response = do
  let responseHeaders = HTTP.responseHeaders response
  -- [WARN]: response headers are always lowercased
  let headerNameLower = String.toLower headerName
  Object.lookup headerNameLower responseHeaders

getHeaderOrThrow :: String -> HTTP.Response -> Effect String
getHeaderOrThrow headerName =
  maybe (throwException (error $ "Required header " <> headerName <> " is missing")) pure <<<
      getHeader headerName

foreign import split2 :: Effect Duplex
