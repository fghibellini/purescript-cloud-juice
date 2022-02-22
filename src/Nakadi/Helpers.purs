module Nakadi.Helpers where

import Prelude

import Data.Either (Either(..))
import Data.Function (on)
import Data.JSDate (JSDate, getTime)
import Data.Time.Duration (Milliseconds(..))
import Json.Schema (JsonSchema)
import Nakadi.Types (EventType, StringSchema(..))

getSchema :: EventType -> Either String JsonSchema
getSchema = case _ of
  { schema: { schema: ValidStringSchema jsonSchema }} -> Right jsonSchema
  { schema: { schema: BrokenStringSchema jsonSchema }} -> Left jsonSchema

diffJSDate :: JSDate -> JSDate -> Milliseconds
diffJSDate a b = Milliseconds $ on (-) getTime a b
