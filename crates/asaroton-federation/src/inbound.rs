//! Authenticating incoming federation requests.

use ruma::OwnedServerName;

use crate::{Federation, Result};

impl Federation {
    /// Verify the `X-Matrix` signature of an incoming request and return its origin.
    pub async fn authenticate_request(
        &self,
        request: &http::Request<&[u8]>,
    ) -> Result<OwnedServerName> {
        self.client.verify_inbound(self, request).await
    }
}
