use std::convert::TryFrom;

use crate::protocol::responses::*;
use crate::{error::DecodeError, ResponseCode};

impl TryFrom<u16> for ResponseCode {
    type Error = DecodeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            RESPONSE_CODE_OK => Ok(ResponseCode::Ok),
            RESPONSE_CODE_STREAM_DOES_NOT_EXIST => Ok(ResponseCode::StreamDoesNotExist),
            RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS => {
                Ok(ResponseCode::SubscriptionIdAlreadyExists)
            }
            RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST => {
                Ok(ResponseCode::SubscriptionIdDoesNotExist)
            }
            RESPONSE_CODE_STREAM_ALREADY_EXISTS => Ok(ResponseCode::StreamAlreadyExists),
            RESPONSE_CODE_STREAM_NOT_AVAILABLE => Ok(ResponseCode::StreamNotAvailable),
            RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED => {
                Ok(ResponseCode::SaslMechanismNotSupported)
            }
            RESPONSE_CODE_AUTHENTICATION_FAILURE => Ok(ResponseCode::AuthenticationFailure),
            RESPONSE_CODE_SASL_ERROR => Ok(ResponseCode::SaslError),
            RESPONSE_CODE_SASL_CHALLENGE => Ok(ResponseCode::SaslChallange),
            RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK => {
                Ok(ResponseCode::AuthenticationFailureLoopback)
            }
            RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE => Ok(ResponseCode::VirtualHostAccessFailure),
            RESPONSE_CODE_UNKNOWN_FRAME => Ok(ResponseCode::UnknownFrame),
            RESPONSE_CODE_FRAME_TOO_LARGE => Ok(ResponseCode::FrameTooLarge),
            RESPONSE_CODE_INTERNAL_ERROR => Ok(ResponseCode::InternalError),
            RESPONSE_CODE_ACCESS_REFUSED => Ok(ResponseCode::AccessRefused),
            RESPONSE_CODE_PRECONDITION_FAILED => Ok(ResponseCode::PrecoditionFailed),
            RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST => Ok(ResponseCode::PublisherDoesNotExist),
            RESPONSE_CODE_OFFSET_NOT_FOUND => Ok(ResponseCode::OffsetNotFound),
            _ => Err(DecodeError::UnknownResponseCode(value)),
        }
    }
}

impl From<&ResponseCode> for u16 {
    fn from(code: &ResponseCode) -> Self {
        match code {
            ResponseCode::Ok => RESPONSE_CODE_OK,
            ResponseCode::StreamDoesNotExist => RESPONSE_CODE_STREAM_DOES_NOT_EXIST,
            ResponseCode::SubscriptionIdAlreadyExists => {
                RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS
            }
            ResponseCode::SubscriptionIdDoesNotExist => {
                RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST
            }
            ResponseCode::StreamAlreadyExists => RESPONSE_CODE_STREAM_ALREADY_EXISTS,
            ResponseCode::StreamNotAvailable => RESPONSE_CODE_STREAM_NOT_AVAILABLE,
            ResponseCode::SaslMechanismNotSupported => RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED,
            ResponseCode::AuthenticationFailure => RESPONSE_CODE_AUTHENTICATION_FAILURE,
            ResponseCode::SaslError => RESPONSE_CODE_SASL_ERROR,
            ResponseCode::SaslChallange => RESPONSE_CODE_SASL_CHALLENGE,
            ResponseCode::AuthenticationFailureLoopback => {
                RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK
            }
            ResponseCode::VirtualHostAccessFailure => RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE,
            ResponseCode::UnknownFrame => RESPONSE_CODE_UNKNOWN_FRAME,
            ResponseCode::FrameTooLarge => RESPONSE_CODE_FRAME_TOO_LARGE,
            ResponseCode::InternalError => RESPONSE_CODE_INTERNAL_ERROR,
            ResponseCode::AccessRefused => RESPONSE_CODE_ACCESS_REFUSED,
            ResponseCode::PrecoditionFailed => RESPONSE_CODE_PRECONDITION_FAILED,
            ResponseCode::PublisherDoesNotExist => RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST,
            ResponseCode::OffsetNotFound => RESPONSE_CODE_OFFSET_NOT_FOUND,
        }
    }
}
