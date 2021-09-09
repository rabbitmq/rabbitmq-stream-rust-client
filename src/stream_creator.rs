use std::collections::HashMap;

use crate::{error::StreamCreateError, Environment};

pub struct StreamCreator {
    pub(crate) env: Environment,
}

impl StreamCreator {
    pub async fn create(self, stream: &str) -> Result<(), StreamCreateError> {
        let client = self.env.create_client().await?;
        let response = client.create_stream(stream, HashMap::new()).await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamCreateError::CreateError {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }
}
