module Nakadi.Types where

import Prelude

import Control.Alt ((<|>))
import Data.JSDate (JSDate)
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Newtype (class Newtype)
import Data.Nullable (toMaybe)
import FlowId (FlowId)
import Foreign (Foreign)
import Json.Schema (JsonSchema)
import Json.Types (AtLeastOne)
import Simple.JSON (class ReadForeign, class WriteForeign, parseJSON, readImpl, writeImpl, writeJSON)

type AdminAuthorization =
  { admins  :: AtLeastOne AuthorizationAttribute
  , readers :: AtLeastOne AuthorizationAttribute
  , writers :: AtLeastOne AuthorizationAttribute
  }

type AuthorizationAttribute =
  { data_type :: DataType
  , value     :: AuthorizationAttributeValue
  }

type BatchItemResponse =
  { eid               :: Maybe Eid -- uuid
  , publishing_status :: String -- enum submitted, failed, aborted
  , step              :: Maybe String
  , detail            :: Maybe String
  }

-- | Usually represents a status transition in a Business process.
type BusinessEvent =
  Foreign

type Cursor =
  { partition :: String -- | Id of the partition pointed to by this cursor
  , offset    :: String -- | offset of the event being pointed to
  }

type CursorCommitResult =
  { cursor :: SubscriptionCursor
  , result :: String  -- | The result of cursor commit. committed/outdated
  }

type CursorDistanceQuery =
  { initial_cursor :: Cursor
  , final_cursor   :: Cursor
  }

type CursorDistanceResult =
  { initial_cursor :: Cursor
  , final_cursor   :: Cursor
  , distance       :: Int
  }

type DataChangeEvent =
  { data_type :: String
  , data_op   :: String -- enum: C,U,D,S
  , metadata  :: EventMetadata
  , data      :: Foreign
  }

data Event
  = BusinessEvent BusinessEvent
  | DataChangeEvent DataChangeEvent
  | UndefinedEvent Foreign
instance writeForeignEvent :: WriteForeign Event where
  writeImpl = case _ of
    BusinessEvent be -> writeImpl be
    DataChangeEvent dce -> writeImpl dce
    UndefinedEvent f -> writeImpl f
instance readForeignEvent :: ReadForeign Event where
  readImpl f = DataChangeEvent <$> readImpl f
           <|> BusinessEvent <$> readImpl f
           <|> pure (UndefinedEvent f)

type EventMetadata =
  { eid                      :: String -- uuid
  , event_type               :: Maybe String
  , occurred_at              :: String -- TODO: Proper date type
  , received_at              :: Maybe String -- TODO: Proper date type -- readOnly
  , version                  :: Maybe String -- readOnly
  , parent_eids              :: Maybe (Array String)
  , flow_id                  :: Maybe FlowId
  , partition                :: Maybe String
  , partition_compaction_key :: Maybe String
  , span_ctx                 :: Maybe Foreign
  }

type EventStreamBatch =
  { cursor :: Cursor
  , info   :: Maybe StreamInfo
  , events :: Array Event
  }

type EventType =
  { name                  :: EventTypeName
  , owning_application    :: OwningApplication -- was nullable in real life call, shouldn't be
  , category              :: String -- | enum undefined, data, business
  , enrichment_strategies :: Maybe (Array String) -- | enum metadata_enrichment
  , partition_strategy    :: Maybe String -- | default random
  , compatibility_mode    :: Maybe String -- | default forward
  , schema                :: EventTypeSchema
  , partition_key_fields  :: Maybe (Array String)
  , cleanup_policy        :: Maybe String -- | x-extensible-enums: delete | compact
  , default_statistic     :: Maybe EventTypeStatistics
  , options               :: Maybe EventTypeOptions
  , authorization         :: Maybe EventTypeAuthorization
  , ordering_key_fields   :: Maybe (Array String)
  , ordering_instance_ids :: Maybe (Array String)
  , audience              :: Maybe String -- x-extensible-enum: component-internal business-unit-internal company-internal external-partner external-public
  , created_at            :: Maybe String
  , updated_at            :: Maybe String
  }

type EventTypeAuthorization =
  { admins  :: AtLeastOne AuthorizationAttribute -- min-size: 1
  , readers :: AtLeastOne AuthorizationAttribute
  , writers :: AtLeastOne AuthorizationAttribute
  }

type EventTypeOptions =
  { retention_time :: Maybe Int -- milliseconds...
  }

type EventTypePartition =
  { event_type :: String
  , partition  :: String
  }

data StringSchema = ValidStringSchema JsonSchema | BrokenStringSchema String
derive instance eqStringSchema :: Eq StringSchema
instance showStringSchema :: Show StringSchema where
  show = case _ of
    ValidStringSchema s -> "(ValidStringSchema " <> show s <> ")"
    BrokenStringSchema s -> show s
instance readForeignStringSchema :: ReadForeign StringSchema where
  readImpl f =
    (ValidStringSchema <$> (readImpl =<< parseJSON =<< readImpl f)) <|>
    (BrokenStringSchema) <$> (readImpl f)
instance writeForeignStringSchema :: WriteForeign StringSchema where
  writeImpl (ValidStringSchema schema) = writeImpl (writeJSON schema)
  writeImpl (BrokenStringSchema schema) = writeImpl schema

type EventTypeSchema =
  { version    :: Maybe String
  , created_at :: Maybe String
  , type       :: String -- enum: json_schema
  , schema     :: StringSchema
  }

type EventTypeStatistics =
  { messages_per_minute :: Int
  , message_size        :: Int
  , read_parallelism    :: Int
  , write_parallelism   :: Int
  }

type Feature =
  { feature :: String
  , enabled :: Boolean
  }

type Metrics = Foreign

type PaginationLink = { href :: Maybe String }

type PaginationLinks =
  { prev :: Maybe PaginationLink
  , next :: Maybe PaginationLink
  }

type Partition =
  { partition               :: String
  , oldest_available_offset :: String
  , newest_available_offset :: String
  , unconsumed_events       :: Maybe Int
  }

type Problem =
  { type     :: Maybe String
  , title    :: String
  , status   :: Int
  , detail   :: Maybe String
  , instance :: Maybe String
  }

type ShiftedCursor =
  { partition :: String -- | Id of the partition pointed to by this cursor
  , offset    :: String -- | offset of the event being pointed to
  , shift     :: Int
  }

type Storage =
  { id                  :: String
  , storage_type        :: String
  , kafka_configuration ::
    { exhibitor_address :: Maybe String
    , exhibitor_port    :: Maybe String
    , zk_address        :: Maybe String
    , zk_path           :: Maybe String
    }
  }

type StreamInfo = Foreign

type Subscription =
  { id                 :: Maybe SubscriptionId
  , owning_application :: OwningApplication
  , event_types        :: Array EventTypeName
  , consumer_group     :: Maybe String
  , created_at         :: Maybe String
  , read_from          :: Maybe String
  , initial_cursors    :: Maybe (Array SubscriptionCursorWithoutToken)
  , status             :: Maybe (Array SubscriptionEventTypeStatus)
  , authorization      :: Maybe SubscriptionAuthorization
  }

type StreamParameters =
  { partitions             :: Maybe (Array EventTypePartition)
  , max_uncommitted_events :: Maybe Int
  , batch_limit            :: Maybe Int
  , stream_limit           :: Maybe Int
  , batch_flush_timeout    :: Maybe Int
  , stream_timeout         :: Maybe Int
  , commit_timeout         :: Maybe Int
  }

type SubscriptionAuthorization =
  { admins  :: AtLeastOne AuthorizationAttribute
  , readers :: AtLeastOne AuthorizationAttribute
  }

type SubscriptionCursor =
  { partition    :: String -- | Id of the partition pointed to by this cursor
  , offset       :: String -- | offset of the event being pointed to
  , event_type   :: String
  , cursor_token :: String
  }

type SubscriptionCursorWithoutToken =
  { partition  :: String -- | Id of the partition pointed to by this cursor
  , offset     :: String -- | offset of the event being pointed to
  , event_type :: String
  }

type SubscriptionEventStreamBatch =
  { cursor :: SubscriptionCursor
  , info   :: Maybe StreamInfo
  , events :: Maybe (Array Event)
  }

type SubscriptionEventTypeStats =
  { event_type :: EventTypeName
  , partitions :: Array
    { partition            :: String
    , state                :: String
    , unconsumed_events    :: Maybe Number
    , consumer_lag_seconds :: Maybe Number
    , stream_id            :: Maybe String
    , assignment_type      :: Maybe String
    }
  }

type SubscriptionEventTypeStatus =
  { event_type :: EventTypeName
  , partitions :: Array
    { partition       :: String
    , state           :: String
    , stream_id       :: Maybe String
    , assignment_type :: Maybe String
    }
  }

newtype DataType = DataType String
derive instance ntDataType :: Newtype DataType _
derive newtype instance eqDataType :: Eq DataType
derive newtype instance ordDataType :: Ord DataType
derive newtype instance showDataType :: Show DataType
derive newtype instance readForeignDataType :: ReadForeign DataType
derive newtype instance writeForeignDataType :: WriteForeign DataType

newtype AuthorizationAttributeValue = AuthorizationAttributeValue String
derive instance ntAuthorizationAttributeValue :: Newtype AuthorizationAttributeValue _
derive newtype instance eqAuthorizationAttributeValue :: Eq AuthorizationAttributeValue
derive newtype instance ordAuthorizationAttributeValue :: Ord AuthorizationAttributeValue
derive newtype instance showAuthorizationAttributeValue :: Show AuthorizationAttributeValue
derive newtype instance readForeignAuthorizationAttributeValue :: ReadForeign AuthorizationAttributeValue
derive newtype instance writeForeignAuthorizationAttributeValue :: WriteForeign AuthorizationAttributeValue

newtype Eid = Eid String
derive instance ntEid :: Newtype Eid _
derive newtype instance eqEid :: Eq Eid
derive newtype instance ordEid :: Ord Eid
derive newtype instance showEid :: Show Eid
derive newtype instance readForeignEid :: ReadForeign Eid
derive newtype instance writeForeignEid :: WriteForeign Eid

newtype EventTypeName = EventTypeName String
derive instance ntEventTypeName :: Newtype EventTypeName _
derive newtype instance eqEventTypeName :: Eq EventTypeName
derive newtype instance ordEventTypeName :: Ord EventTypeName
derive newtype instance showEventTypeName :: Show EventTypeName
derive newtype instance readForeignEventTypeName :: ReadForeign EventTypeName
derive newtype instance writeForeignEventTypeName :: WriteForeign EventTypeName

newtype OwningApplication = OwningApplication String
derive instance ntOwningApplication :: Newtype OwningApplication _
derive newtype instance eqOwningApplication :: Eq OwningApplication
derive newtype instance ordOwningApplication :: Ord OwningApplication
derive newtype instance showOwningApplication :: Show OwningApplication
instance readForeignOwningApplication :: ReadForeign OwningApplication where
  readImpl f = do
    nullable <- readImpl f
    pure (OwningApplication $ fromMaybe "" (toMaybe nullable))
derive newtype instance writeForeignOwningApplication :: WriteForeign OwningApplication

newtype SubscriptionId = SubscriptionId String
derive instance ntSubscriptionId :: Newtype SubscriptionId _
derive newtype instance eqSubscriptionId :: Eq SubscriptionId
derive newtype instance ordSubscriptionId :: Ord SubscriptionId
derive newtype instance showSubscriptionId :: Show SubscriptionId
derive newtype instance readForeignSubscriptionId :: ReadForeign SubscriptionId
derive newtype instance writeForeignSubscriptionId :: WriteForeign SubscriptionId

newtype XNakadiStreamId = XNakadiStreamId String
derive instance ntXNakadiStreamId :: Newtype XNakadiStreamId _
derive newtype instance eqXNakadiStreamId :: Eq XNakadiStreamId
derive newtype instance ordXNakadiStreamId :: Ord XNakadiStreamId
derive newtype instance showXNakadiStreamId :: Show XNakadiStreamId
derive newtype instance readForeignXNakadiStreamId :: ReadForeign XNakadiStreamId
derive newtype instance writeForeignXNakadiStreamId :: WriteForeign XNakadiStreamId

type SubscriptionStats =
  { subscriptionStartTime :: JSDate
  , lastCommitTime :: Maybe JSDate
  , receivedBatchCount :: Int
  , committedBatchCount :: Int
  , committedEventCount :: Int
  }

emptySubscriptionStats :: JSDate -> SubscriptionStats
emptySubscriptionStats t0 =
  { subscriptionStartTime: t0
  , lastCommitTime: Nothing
  , receivedBatchCount: 0
  , committedBatchCount: 0
  , committedEventCount: 0
  }
