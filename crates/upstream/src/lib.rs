//! How ncro talks to its upstreams. Each configured upstream gets one
//! [`Backend`], chosen by the scheme of its URL and shared by every caller.

mod http;
mod s3;

use std::{sync::Arc, time::Duration};

use aws_credential_types::provider::error::CredentialsError;
use aws_sigv4::http_request::SigningError;
use bytes::Bytes;
use dashmap::DashMap;
use flub::Error;
use ncro_config::{Location, UpstreamConfig};
use reqwest::{Method, StatusCode, header::HeaderMap};
use rustls::crypto::ring;

use crate::{http::HttpBackend, s3::S3Backend};

#[derive(Debug, Error)]
pub enum BackendError {
  #[error("build HTTP client for upstream {upstream}")]
  HttpClient {
    upstream: String,
    source:   reqwest::Error,
  },
  #[error("load AWS credentials for S3 upstream {upstream}")]
  Credentials {
    upstream: String,
    source:   CredentialsError,
  },
  #[error("sign S3 request to {url}")]
  Sign {
    url:    String,
    source: SigningError,
  },
  #[error(
    "S3 key {key:?} has a . or .. segment, which URL parsing would collapse"
  )]
  DotSegment { key: String },
  #[error("request to {url}")]
  Request {
    url:    String,
    source: reqwest::Error,
  },
  #[error("{url} returned {status}")]
  Status { url: String, status: StatusCode },
  #[error("read body from {url}")]
  Body {
    url:    String,
    source: reqwest::Error,
  },
}

pub enum Backend {
  Http(HttpBackend),
  S3(Box<S3Backend>),
}

impl Backend {
  /// Sends `method` for `path`, which starts with `/`. A missing object is
  /// `None`.
  ///
  /// # Errors
  ///
  /// Fails when the upstream can't be reached or answers with any other
  /// non-success status.
  pub async fn send(
    &self,
    method: Method,
    path: &str,
    headers: HeaderMap,
    timeout: Option<Duration>,
  ) -> Result<Option<reqwest::Response>, BackendError> {
    match self {
      Self::Http(backend) => backend.send(method, path, headers, timeout).await,
      Self::S3(backend) => backend.send(method, path, headers, timeout).await,
    }
  }

  /// Fetches the whole object at `path`.
  ///
  /// # Errors
  ///
  /// Fails like [`Backend::send`], or when the body can't be read.
  pub async fn get_bytes(
    &self,
    path: &str,
    headers: HeaderMap,
    timeout: Option<Duration>,
  ) -> Result<Option<Bytes>, BackendError> {
    let Some(resp) = self.send(Method::GET, path, headers, timeout).await?
    else {
      return Ok(None);
    };
    let url = resp.url().to_string();
    let bytes = resp
      .bytes()
      .await
      .map_err(|source| BackendError::Body { url, source })?;
    Ok(Some(bytes))
  }

  #[must_use]
  pub const fn kind(&self) -> &'static str {
    match self {
      Self::Http(_) => "http",
      Self::S3(_) => "s3",
    }
  }

  #[must_use]
  pub const fn is_authenticated(&self) -> bool {
    match self {
      Self::Http(backend) => backend.is_authenticated(),
      Self::S3(_) => false,
    }
  }
}

/// Every registered upstream's backend, shared by the router, prober and
/// server so each upstream's clients and credentials exist once.
#[derive(Clone)]
pub struct Upstreams {
  backends: Arc<DashMap<String, Arc<Backend>>>,
  http:     reqwest::Client,
}

impl Upstreams {
  /// # Errors
  ///
  /// Fails when the shared HTTP client can't be built.
  pub fn new() -> Result<Self, reqwest::Error> {
    let _ = ring::default_provider().install_default();

    Ok(Self {
      backends: Arc::default(),
      http:     reqwest::Client::builder().build()?,
    })
  }

  pub fn register(&self, upstream: &UpstreamConfig) {
    let backend = match &upstream.location {
      Location::Http => {
        Backend::Http(HttpBackend::new(
          upstream.url.clone(),
          self.http.clone(),
          upstream.username.clone(),
          upstream.password.clone(),
        ))
      },
      Location::S3(config) => {
        Backend::S3(Box::new(S3Backend::new(
          upstream.url.clone(),
          config.clone(),
        )))
      },
    };
    self
      .backends
      .insert(upstream.url.clone(), Arc::new(backend));
  }

  /// The backend registered for `url`, or plain unauthenticated HTTP for
  /// upstreams that were never configured, like discovered peers.
  #[must_use]
  pub fn get(&self, url: &str) -> Arc<Backend> {
    self.backends.get(url).map_or_else(
      || {
        Arc::new(Backend::Http(HttpBackend::new(
          url.to_string(),
          self.http.clone(),
          String::new(),
          None,
        )))
      },
      |backend| Arc::clone(&backend),
    )
  }
}
