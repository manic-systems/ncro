use std::{
  sync::{Arc, Mutex, PoisonError},
  time::{Duration, Instant},
};

use reqwest::{ClientBuilder, Method, StatusCode, header::HeaderMap};

use crate::BackendError;

const MAX_CLIENT_AGE: Duration = Duration::from_mins(1);

/// A reqwest client rebuilt every [`MAX_CLIENT_AGE`].
#[derive(Clone)]
pub struct RotatingClient {
  build:   fn() -> ClientBuilder,
  current: Arc<Mutex<Built>>,
}

struct Built {
  at:     Instant,
  client: reqwest::Client,
}

impl RotatingClient {
  pub fn new(build: fn() -> ClientBuilder) -> reqwest::Result<Self> {
    let client = build().build()?;
    Ok(Self {
      build,
      current: Arc::new(Mutex::new(Built {
        at: Instant::now(),
        client,
      })),
    })
  }

  pub(crate) fn client(
    &self,
    upstream: &str,
  ) -> Result<reqwest::Client, BackendError> {
    let mut current =
      self.current.lock().unwrap_or_else(PoisonError::into_inner);
    if current.at.elapsed() >= MAX_CLIENT_AGE {
      let client = (self.build)().build().map_err(|source| {
        BackendError::HttpClient {
          upstream: upstream.to_owned(),
          source,
        }
      })?;
      *current = Built {
        at: Instant::now(),
        client,
      };
    }
    Ok(current.client.clone())
  }
}

/// A Nix binary cache served over HTTP, with optional basic auth.
pub struct HttpBackend {
  url:      String,
  client:   RotatingClient,
  username: String,
  password: Option<String>,
}

impl HttpBackend {
  pub(crate) const fn new(
    url: String,
    client: RotatingClient,
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
    let client = self.client.client(&self.url)?;
    let url = format!("{}{path}", self.url);
    let mut req = client.request(method, &url).headers(headers);
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
