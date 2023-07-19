/// Low level stream commands
///
#[allow(unused)]
pub mod commands {
    pub const COMMAND_DECLARE_PUBLISHER: u16 = 1;
    pub const COMMAND_PUBLISH: u16 = 2;
    pub const COMMAND_PUBLISH_CONFIRM: u16 = 3;
    pub const COMMAND_PUBLISH_ERROR: u16 = 4;
    pub const COMMAND_QUERY_PUBLISHER_SEQUENCE: u16 = 5;
    pub const COMMAND_DELETE_PUBLISHER: u16 = 6;
    pub const COMMAND_SUBSCRIBE: u16 = 7;
    pub const COMMAND_DELIVER: u16 = 8;
    pub const COMMAND_CREDIT: u16 = 9;
    pub const COMMAND_STORE_OFFSET: u16 = 10;
    pub const COMMAND_QUERY_OFFSET: u16 = 11;
    pub const COMMAND_UNSUBSCRIBE: u16 = 12;
    pub const COMMAND_CREATE_STREAM: u16 = 13;
    pub const COMMAND_DELETE_STREAM: u16 = 14;
    pub const COMMAND_METADATA: u16 = 15;
    pub const COMMAND_METADATA_UPDATE: u16 = 16;
    pub const COMMAND_PEER_PROPERTIES: u16 = 17;
    pub const COMMAND_SASL_HANDSHAKE: u16 = 18;
    pub const COMMAND_SASL_AUTHENTICATE: u16 = 19;
    pub const COMMAND_TUNE: u16 = 20;
    pub const COMMAND_OPEN: u16 = 21;
    pub const COMMAND_CLOSE: u16 = 22;
    pub const COMMAND_HEARTBEAT: u16 = 23;
}

// server responses
//
#[allow(unused)]
pub mod responses {
    pub const RESPONSE_CODE_OK: u16 = 1;
    pub const RESPONSE_CODE_STREAM_DOES_NOT_EXIST: u16 = 2;
    pub const RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS: u16 = 3;
    pub const RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST: u16 = 4;
    pub const RESPONSE_CODE_STREAM_ALREADY_EXISTS: u16 = 5;
    pub const RESPONSE_CODE_STREAM_NOT_AVAILABLE: u16 = 6;
    pub const RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED: u16 = 7;
    pub const RESPONSE_CODE_AUTHENTICATION_FAILURE: u16 = 8;
    pub const RESPONSE_CODE_SASL_ERROR: u16 = 9;
    pub const RESPONSE_CODE_SASL_CHALLENGE: u16 = 10;
    pub const RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK: u16 = 11;
    pub const RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE: u16 = 12;
    pub const RESPONSE_CODE_UNKNOWN_FRAME: u16 = 13;
    pub const RESPONSE_CODE_FRAME_TOO_LARGE: u16 = 14;
    pub const RESPONSE_CODE_INTERNAL_ERROR: u16 = 15;
    pub const RESPONSE_CODE_ACCESS_REFUSED: u16 = 16;
    pub const RESPONSE_CODE_PRECONDITION_FAILED: u16 = 17;
    pub const RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST: u16 = 18;
    pub const RESPONSE_CODE_OFFSET_NOT_FOUND: u16 = 19;
}

// protocol version between client and server

#[allow(unused)]
pub mod version {
    pub const PROTOCOL_VERSION: u16 = 1;
}
