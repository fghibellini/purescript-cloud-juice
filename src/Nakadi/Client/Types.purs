module Nakadi.Client.Types
  ( Env
  , LogWarnFn
  , NakadiResponse
  , SpanCtx(..)
  )
  where

import Prelude

import Data.Either (Either)
import Data.Maybe (Maybe)
import Data.Newtype (class Newtype)
import Data.Variant (Variant)
import Effect (Effect)
import FlowId (FlowId)
import Nakadi.Errors (E401)
import Nakadi.Types (Problem)

type LogWarnFn = Maybe Problem -> String -> Effect Unit

type Env r =
  { flowId  :: FlowId
  , baseUrl :: String
  , port    :: Int
  , token   :: Effect String
  , logWarn :: Maybe Problem -> String -> Effect Unit
  | r
  }

type NakadiResponse r a = Either (Variant (unauthorized :: E401 | r)) a

newtype SpanCtx = SpanCtx String

derive instance newtypeSpanCtx :: Newtype SpanCtx _
