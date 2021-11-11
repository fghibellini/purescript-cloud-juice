
module Test.Express.Curry (write) where

import Prelude

import Data.Function.Uncurried (Fn2, runFn2)
import Effect (Effect)
import Effect.Class (liftEffect)
import Node.Express.Handler (Handler, HandlerM(..))
import Node.Express.Types (Response)


write :: forall a. a -> Handler
write data_ = HandlerM \_ resp _ ->
    liftEffect $ runFn2 _write resp data_

foreign import _write :: forall a. Fn2 Response a (Effect Unit)

