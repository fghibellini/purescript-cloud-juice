module Nakadi.Client.Internal where

import Prelude

import Affjax (Request, Response, printError)
import Affjax as AX
import Affjax.RequestBody as RequestBody
import Affjax.RequestHeader (RequestHeader(..))
import Affjax.ResponseFormat as ResponseFormat
import Affjax.StatusCode (StatusCode(..))
import Control.Monad.Error.Class (class MonadThrow, throwError)
import Control.Monad.Reader (class MonadAsk, ask)
import Data.Array (concat)
import Data.Either (Either(..), either)
import Data.Foldable (class Foldable, foldMap)
import Data.HTTP.Method (Method(..))
import Data.Maybe (Maybe(..), maybe)
import Data.Newtype (unwrap)
import Data.Tuple (Tuple(..), uncurry)
import Data.Variant (SProxy(..), Variant, inj)
import Effect.Aff.Class (class MonadAff, liftAff)
import Effect.Class (liftEffect)
import Effect.Exception (Error, error)
import Foreign (ForeignError, renderForeignError)
import Foreign.Object as Obj
import Nakadi.Client.Types (Env, SpanCtx(..))
import Nakadi.Types (Problem(..))
import Simple.JSON (class ReadForeign, class WriteForeign, readJSON, writeJSON)

request :: ∀ a m. MonadAff m => Request a -> m (Either AX.Error (Response a))
request = liftAff <<< AX.request

baseHeaders :: ∀ r m . MonadAsk (Env r) m => MonadAff m => m (Array (Tuple String String))
baseHeaders = do
  env <- ask
  token <- liftEffect $ env.token
  pure [ Tuple "X-Flow-ID" (unwrap env.flowId)
       , Tuple "Authorization" token
       ]

baseRequest :: ∀ r m . MonadAsk (Env r) m => MonadAff m => Method -> String -> m (Request String)
baseRequest method path = do
  env <- ask
  headers <- baseHeaders <#> (map \(Tuple k v) -> RequestHeader k v)
  let port = show env.port
  pure $
    AX.defaultRequest
    { method = Left method
    , responseFormat = ResponseFormat.string
    , headers = headers
    , url = env.baseUrl <> ":" <> port <> path
    }

getRequest :: ∀ r m. MonadAsk (Env r) m => MonadAff m => String -> m (Request String)
getRequest = baseRequest GET

deleteRequest :: ∀ r m. MonadAsk (Env r) m => MonadAff m => String -> m (Request String)
deleteRequest = baseRequest DELETE

writeRequest :: ∀ a m r. WriteForeign a => MonadAsk (Env r) m => MonadAff m => Method -> String -> Maybe SpanCtx -> a -> m (Request String)
writeRequest method path spanCtx content = do
  req <- baseRequest method path
  let contentString = writeJSON content -- # spy "Body to send"
  let body = Just (RequestBody.string contentString)
  let headers = concat
        [ req.headers
        , [ RequestHeader "Content-Type" "application/json" ]
        , maybe [] (\(SpanCtx obj) -> uncurry RequestHeader <$> Obj.toUnfoldable obj) spanCtx
        ]
  pure $ req
    { content = body
    , headers = headers
    }

postRequest :: ∀ a m r . WriteForeign a => MonadAsk (Env r) m => MonadAff m => String -> Maybe SpanCtx -> a -> m (Request String)
postRequest = writeRequest POST

putRequest :: ∀ a m r . WriteForeign a => MonadAsk (Env r) m => MonadAff m => String -> Maybe SpanCtx -> a -> m (Request String)
putRequest = writeRequest PUT

formatErr :: ∀ m a. MonadThrow Error m => AX.Error -> m a
formatErr = throwError <<< error <<< printError

jsonErr :: ∀ m f a.  MonadThrow Error m => Foldable f => f ForeignError -> m a
jsonErr = throwError <<< error <<< foldMap renderForeignError

type AjaxCallResult a = Either (Variant (ajaxError :: AX.Error, httpError :: Problem)) a

ajaxAssert2xx :: Either AX.Error (Response String) -> AjaxCallResult String
ajaxAssert2xx = case _ of
  Right { body, status: StatusCode code } ->
    if code # between 200 299
      then Right body
      else Left $ inj (SProxy :: SProxy "httpError") $ deserialiseProblem code body
  Left error ->
    Left $ inj (SProxy :: SProxy "ajaxError") error

-- A request can have 3 possible outcomes (from the perspective of this function)
--
-- 1. successfull network call & status code == 2xx
--    => in this case we simply parse the response JSON and return it in a `Right`
-- 2. successfull network call & status code != 2xx
--    => here we try to parse the response body into a `Problem`` (which has two constructors,
--       one for responses from Nakadi and one for "malformed responses" e.g. something received from a load balancer)
-- 3. unsuccessfull network call i.e. we were not able to receive any response
--    => this ends up producing the `Left (Variant (ajaxError :: AX.Error, ...))` of the result.
--
-- WARNING: this functions throws if the 200 response JSON cannot be parsed
processResponseWithBody :: ∀ m a . MonadThrow Error m => ReadForeign a => Either AX.Error (Response String) -> m (AjaxCallResult a)
processResponseWithBody res = case ajaxAssert2xx res of
  Right body -> Right <$> readJsonOrThrow body
  Left err -> pure (Left err)

-- Same as `processResponseWithBody` but the body a successfull call is ignored
processResponseNoBody :: Either AX.Error (Response String) -> AjaxCallResult Unit
processResponseNoBody res = unit <$ ajaxAssert2xx res

deserialiseProblem :: Int -> String -> Problem
deserialiseProblem status responseText = case readJSON responseText of
  Left _ -> HttpErrorResponse { status, body: responseText }
  Right parsedNakadiRes -> NakadiErrorResponse parsedNakadiRes

readJsonOrThrow :: ∀ m a. MonadThrow Error m => ReadForeign a => String -> m a
readJsonOrThrow = readJSON >>> either jsonErr pure
