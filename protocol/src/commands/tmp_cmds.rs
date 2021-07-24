// TODO complete (watch over TODOs) and split to one file per command

pub struct DeclarePublisherRequest<'a> {
    // => Key Version CorrelationId PublisherId [PublisherReference] Stream
    CorrelationId: u32,
    PublisherId: u8,
    PublisherReference: &'a str, // max 256 characters
    Stream: &'a str,
}

pub struct DeclarePublisherResponse {
    // => Key Version CorrelationId ResponseCode PublisherId
    CorrelationId: u32,
    ResponseCode: u16,
}

struct PublishedMessage; // TODO

pub struct Publish {
    // => Key Version PublisherId PublishedMessages
    PublisherId: u8,
    PublishedMessages: Vec<PublishedMessage>,
    PublishedMessage: u32, // TODO PublishingId Message
    PublishingId: u64,
    Message: bytes,
}

pub struct PublishConfirm {
    // => Key Version PublishingIds
    PublisherId: u8,
    PublishingIds: Vec<u64>, // to correlate with the messages sent
}

pub struct PublishError {
    // => Key Version [PublishingError]
    PublisherId: u8,
    PublishingError: u32, // TODO PublishingId Code
    PublishingId: u64,
    Code: u16, // code to identify the problem
}

pub struct QueryPublisherRequest<'a> {
    // => Key Version CorrelationId PublisherReference Stream
    CorrelationId: u32,
    PublisherReference: &'a str, // max 256 characters
    Stream: &'a str,
}

pub struct QueryPublisherResponse {
    // => Key Version CorrelationId ResponseCode Sequence
    CorrelationId: u32,
    ResponseCode: u16,
    Sequence: u64,
}

pub struct DeletePublisherRequest {
    // => Key Version CorrelationId PublisherId
    CorrelationId: u32,
    PublisherId: u8,
}

pub struct DeletePublisherResponse {
    // => Key Version CorrelationId ResponseCode
    CorrelationId: u32,
    ResponseCode: u16,
}

struct Property; // TODO

pub struct Subscribe<'a> {
    // => Key Version CorrelationId SubscriptionId Stream OffsetSpecification Credit Properties
    CorrelationId: u32,       // correlation id to correlate the response
    SubscriptionId: u8,       // client-supplied id to identify the subscription
    Stream: &'a str,          // the name of the stream
    OffsetSpecification: u32, // TODO OffsetType Offset
    OffsetType: u16,          // 1 (first), 2 (last), 3 (next), 4 (offset), 5 (timestamp)
    Offset: u64,              // TODO ??? (for offset) | int64 (for timestamp)
    Credit: u16,
    Properties: Vec<Property>,
    Property: u32, // TODO Key Value
    Value: &'a str,
}

struct Message; // TODO
struct EntryTypeAndSize; // TODO

//NB: See the https://github.com/rabbitmq/osiris/blob/348db0528986d6025b823bcf1ae0570aa63f5e25/src/osiris_log.erl#L49-L81[Osiris project]
//for details on the structure of messages.
pub struct Deliver {
    // => Key Version SubscriptionId OsirisChunk
    SubscriptionId: u8,
    // TODO OsirisChunk : MagicVersion NumEntries NumRecords Epoch ChunkFirstOffset ChunkCrc DataLength Messages
    MagicVersion: i8,
    NumEntries: u16,
    NumRecords: u32,
    Epoch: u64,
    ChunkFirstOffset: u64,
    ChunkCrc: i32,
    DataLength: u32,
    Messages: Vec<Message>, // no int32 for the size for this array
    Message: EntryTypeAndSize,
    Data: bytes,
}

pub struct Credit {
    // => Key Version SubscriptionId Credit
    SubscriptionId: u8,
    Credit: u16, // the number of chunks that can be sent
}

// NB: the server sent a response only in case of problem, e.g. crediting an unknown subscription.
pub struct CreditResponse {
    // => Key Version ResponseCode SubscriptionId
    ResponseCode: u16,
    SubscriptionId: u8,
}

pub struct StoreOffset<'a> {
    // => Key Version Reference Stream Offset
    Reference: &'a str, // max 256 characters
    SubscriptionId: u8,
    Offset: u64,
}

pub struct QueryOffsetRequest<'a> {
    // => Key Version CorrelationId Reference Stream
    CorrelationId: u32,
    Reference: &'a str, // max 256 characters
    Stream: &'a str,
}

pub struct QueryOffsetResponse {
    // => Key Version CorrelationId ResponseCode Offset
    CorrelationId: u32,
    ResponseCode: u16,
    Offset: u64,
}

pub struct Unsubscribe {
    // => Key Version CorrelationId SubscriptionId
    CorrelationId: u32,
    SubscriptionId: u8,
}

struct Argument; // TODO

pub struct Create<'a> {
    // => Key Version CorrelationId Stream Arguments
    CorrelationId: u32,
    Stream: &'a str,
    Arguments: Vec<Argument>,
    Argument: HashMap<u32, u32>, // TODO Key Value,
    Value: &'a str,
}

pub struct Delete {
    // => Key Version CorrelationId Stream
    CorrelationId: u32,
    Stream: &'a str,
}

pub struct MetadataQuery {
    // => Key Version CorrelationId [Stream]
    CorrelationId: u32,
    Stream: &'a str,
}

struct Broker<'a> {
    Reference: u16,
    Host: &'a str,
    Port: u32,
}

struct StreamMetadata<'a> {
    StreamName: &'a str,
    ResponseCode: u16,
    LeaderReference: u16,
    ReplicasReferences: Vec<u16>,
}

pub struct MetadataResponse {
    // => Key Version CorrelationId [Broker] [StreamMetadata]
    CorrelationId: u32,
    Broker: Broker,
    StreamMetadata: StreamMetadata,
}

pub struct MetadataUpdate<'a> {
    // => Key Version MetadataInfo,,
    MetadataInfo: u32, // TODO ??? Code Stream
    Code: u16,         // code to identify the information
    Stream: &'a str,   // the stream implied
}

struct PeerProperty;

pub struct PeerPropertiesRequest<'a> {
    // => Key Version PeerProperties
    CorrelationId: u32,
    PeerProperties: Vec<PeerProperty>,
    PeerProperty: HashMap<u32, u32>, // TODO
    Value: &'a str,
}

pub struct PeerPropertiesResponse<'a> {
    // => Key Version CorrelationId ResponseCode PeerProperties
    CorrelationId: u32,
    ResponseCode: u16,
    PeerProperties: Vec<PeerProperty>,
    PeerProperty: HashMap<u32, u32>, // TODO
    Value: &'a str,
}

pub struct SaslHandshakeRequest {
    // => Key Version CorrelationId Mechanism
    CorrelationId: u32,
}

pub struct SaslHandshakeResponse<'a> {
    // => Key Version CorrelationId ResponseCode [Mechanism]
    CorrelationId: u32,
    ResponseCode: u16,
    Mechanism: &'a str,
}

pub struct SaslAuthenticateRequest<'a> {
    // => Key Version CorrelationId Mechanism SaslOpaqueData
    CorrelationId: u32,
    Mechanism: &'a str,
    SaslOpaqueData: bytes,
}

pub struct SaslAuthenticateResponse {
    // => Key Version CorrelationId ResponseCode SaslOpaqueData
    CorrelationId: u32,
    ResponseCode: u16,
    SaslOpaqueData: bytes,
}

pub struct TuneRequest {
    // => Key Version FrameMax Heartbeat
    FrameMax: u32,  // in bytes, 0 means no limit
    Heartbeat: u32, // in seconds, 0 means no heartbeat
}

// TODO we should alias TuneResponse to TuneRequest

pub struct CloseRequest<'a> {
    // => Key Version CorrelationId ClosingCode ClosingReason
    CorrelationId: u32,
    ClosingCode: u16,
    ClosingReason: &'a str,
}

pub struct CloseResponse {
    // => Key Version CorrelationId ResponseCode
    CorrelationId: u32,
    ResponseCode: u16,
}

pub struct RouteQuery<'a> {
    // => Key Version CorrelationId RoutingKey SuperStream
    CorrelationId: u32,
    RoutingKey: &'a str,
    SuperStream: &'a str,
}

pub struct RouteResponse<'a> {
    // => Key Version CorrelationId Stream
    CorrelationId: u32,
    Stream: &'a str,
}

pub struct PartitionsQuery<'a> {
    // => Key Version CorrelationId SuperStream
    CorrelationId: u32,
    SuperStream: &'a str,
}

pub struct PartitionsResponse<'a> {
    // => Key Version CorrelationId [Stream]
    CorrelationId: u32,
    Stream: &'a str,
}
