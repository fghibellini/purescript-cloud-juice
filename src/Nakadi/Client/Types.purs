module Nakadi.Client.Types
  ( Env
  , LogWarnFn
  , NakadiResponse
  , SpanCtx(..)
  )
  where

import Prelude

import Affjax as AX
import Data.Either (Either)
import Data.Maybe (Maybe)
import Data.Newtype (class Newtype)
import Data.Time.Duration (Milliseconds)
import Data.Variant (Variant)
import Effect (Effect)
import FlowId (FlowId)
import Foreign.Object (Object)
import Nakadi.Errors (E401, E_UNEXPECTED)
import Nakadi.Types (Problem)

type LogWarnFn = Maybe Problem -> String -> Effect Unit

type Env r =
  { flowId  :: FlowId
  , baseUrl :: String
  , port    :: Int
  , token   :: Effect String
  , timeout   :: Milliseconds
  , logWarn :: Maybe Problem -> String -> Effect Unit
  | r
  }

type NakadiResponse r a = Either (Variant (ajaxError :: AX.Error, unexpected :: E_UNEXPECTED, unauthorized :: E401 | r)) a

newtype SpanCtx = SpanCtx (Object String)

derive instance newtypeSpanCtx :: Newtype SpanCtx _
