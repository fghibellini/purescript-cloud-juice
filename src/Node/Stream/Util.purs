module Node.Stream.Util where

import Prelude

import Data.Function.Uncurried (Fn4, runFn4)

import Data.Options (Option, opt)
import Effect (Effect)
import Effect.Exception (Error)
import Foreign (Foreign)
import Node.Buffer (Buffer)
import Node.HTTP.Client (RequestOptions)

foreign import splitAtNewlineImpl ∷
  Fn4 Buffer Int (Error -> Effect Unit) (Foreign -> Effect Unit) (Effect (Buffer -> Effect Unit))

foreign import allocUnsafeImpl ∷ Int -> Effect Buffer

allocUnsafe ∷ BufferSize -> Effect Buffer
allocUnsafe (BufferSize bs) = allocUnsafeImpl bs

newtype BufferSize = BufferSize Int

splitAtNewline ∷ Buffer -> BufferSize -> (Error -> Effect Unit) -> (Foreign -> Effect Unit) -> Effect (Buffer -> Effect Unit)
splitAtNewline b (BufferSize bs) = runFn4 splitAtNewlineImpl b bs


foreign import data Agent ∷ Type

foreign import newHttpsKeepAliveAgent ∷ Effect Agent
foreign import newHttpKeepAliveAgent ∷ Effect Agent

-- | The agent to use
agent ∷ Option RequestOptions Agent
agent =  opt "agent"
