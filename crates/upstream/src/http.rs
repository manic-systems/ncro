use std::time::Duration;

use reqwest::{Method, StatusCode, header::HeaderMap};

use crate::BackendError;

/// A Nix binary cache served over HTTP, with optional basic auth.
pub struct HttpBackend {
  url:      String,
  client:   reqwest::Client,
  username: String,
  password: Option<String>,
}

impl HttpBackend {
  pub(crate) const fn new(
    url: String,
    client: reqwest::Client,
    username: String,
    password: Option<String>,
  ) -> Self {
    Self {
      url,
      client,
      username,
      password,
    }
  }

  pub(crate) const fn is_authenticated(&self) -> bool {
    !self.username.is_empty()
  }

  pub(crate) async fn send(
    &self,
    method: Method,
    path: &str,
    headers: HeaderMap,
    timeout: Option<Duration>,
  ) -> Result<Option<reqwest::Response>, BackendError> {
    let url = format!("{}{path}", self.url);
    let mut req = self.client.request(method, &url).headers(headers);
    if self.is_authenticated() {
      req = req.basic_auth(&self.username, self.password.as_ref());
    }
    if let Some(timeout) = timeout {
      req = req.timeout(timeout);
    }

    let resp = req.send().await.map_err(|source| {
      BackendError::Request {
        url: url.clone(),
        source,
      }
    })?;
    match resp.status() {
      status if is_miss_status(status) => Ok(None),
      status if status.is_success() => Ok(Some(resp)),
      status => Err(BackendError::Status { url, status }),
    }
  }
}

/// The statuses Nix's HTTP binary cache store also treats as a missing file.
const fn is_miss_status(status: StatusCode) -> bool {
  matches!(
    status,
    StatusCode::NOT_FOUND | StatusCode::FORBIDDEN | StatusCode::GONE
  )
}
