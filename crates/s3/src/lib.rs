use std::{fmt::Display, io};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
  Client,
  config::{Builder, Region},
  primitives::ByteStream,
};
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{Stream, stream};
use ncro_config::S3Config;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum S3Error {
  #[error("unknown S3 upstream {0:?}")]
  UnknownUpstream(String),
  #[error("S3 request failed: {0}")]
  Request(String),
  #[error("read S3 object body: {0}")]
  Body(String),
}

pub struct S3Object {
  pub status:         u16,
  pub body:           ByteStream,
  pub content_type:   Option<String>,
  pub content_length: Option<i64>,
  pub content_range:  Option<String>,
  pub accept_ranges:  Option<String>,
  pub etag:           Option<String>,
  pub last_modified:  Option<String>,
}

pub struct S3ObjectHead {
  pub content_type:   Option<String>,
  pub content_length: Option<i64>,
  pub accept_ranges:  Option<String>,
  pub etag:           Option<String>,
  pub last_modified:  Option<String>,
}

#[derive(Clone, Default)]
pub struct S3ClientPool {
  configs: DashMap<String, S3Config>,
  clients: DashMap<String, Client>,
}

impl S3ClientPool {
  pub fn register(&self, upstream: String, config: S3Config) {
    self.configs.insert(upstream, config);
  }

  #[must_use]
  pub fn contains(&self, upstream: &str) -> bool {
    self.configs.contains_key(upstream)
  }

  /// # Errors
  ///
  /// Returns [`S3Error`] if the upstream is not registered, credentials or
  /// signing fail, or the S3 service returns an error other than missing
  /// object.
  pub async fn head_object(
    &self,
    upstream: &str,
    key: &str,
  ) -> Result<bool, S3Error> {
    self
      .head_object_metadata(upstream, key)
      .await
      .map(|metadata| metadata.is_some())
  }

  /// # Errors
  ///
  /// Returns [`S3Error`] if the upstream is not registered, credentials or
  /// signing fail, or the S3 service returns an error other than missing
  /// object.
  pub async fn head_object_metadata(
    &self,
    upstream: &str,
    key: &str,
  ) -> Result<Option<S3ObjectHead>, S3Error> {
    let config = self.config(upstream)?;
    let client = self.client(upstream, &config).await;
    match client
      .head_object()
      .bucket(config.bucket)
      .key(key)
      .send()
      .await
    {
      Ok(resp) => {
        Ok(Some(S3ObjectHead {
          content_type:   resp.content_type,
          content_length: resp.content_length,
          accept_ranges:  resp.accept_ranges,
          etag:           resp.e_tag,
          last_modified:  resp.last_modified.map(|dt| dt.to_string()),
        }))
      },
      Err(err) if is_not_found(&err) => Ok(None),
      Err(err) => Err(S3Error::Request(err.to_string())),
    }
  }

  /// # Errors
  ///
  /// Returns [`S3Error`] if the upstream is not registered, the object body
  /// cannot be read, credentials or signing fail, or the S3 service returns an
  /// error other than missing object.
  pub async fn get_object_bytes(
    &self,
    upstream: &str,
    key: &str,
  ) -> Result<Option<Vec<u8>>, S3Error> {
    let Some(object) = self.get_object(upstream, key, None).await? else {
      return Ok(None);
    };
    let bytes = object
      .body
      .collect()
      .await
      .map_err(|err| S3Error::Body(err.to_string()))?
      .into_bytes();
    Ok(Some(bytes.to_vec()))
  }

  /// # Errors
  ///
  /// Returns [`S3Error`] if the upstream is not registered, credentials or
  /// signing fail, or the S3 service returns an error other than missing
  /// object.
  pub async fn get_object(
    &self,
    upstream: &str,
    key: &str,
    range: Option<&str>,
  ) -> Result<Option<S3Object>, S3Error> {
    let config = self.config(upstream)?;
    let client = self.client(upstream, &config).await;
    let mut req = client.get_object().bucket(config.bucket).key(key);
    if let Some(range) = range {
      req = req.range(range);
    }
    match req.send().await {
      Ok(resp) => {
        Ok(Some(S3Object {
          status:         if range.is_some() { 206 } else { 200 },
          body:           resp.body,
          content_type:   resp.content_type,
          content_length: resp.content_length,
          content_range:  resp.content_range,
          accept_ranges:  resp.accept_ranges,
          etag:           resp.e_tag,
          last_modified:  resp.last_modified.map(|dt| dt.to_string()),
        }))
      },
      Err(err) if is_not_found(&err) => Ok(None),
      Err(err) => Err(S3Error::Request(err.to_string())),
    }
  }

  pub fn body_stream(
    body: ByteStream,
  ) -> impl Stream<Item = Result<Bytes, io::Error>> {
    stream::unfold(body, |mut body| {
      async move {
        match body.try_next().await {
          Ok(Some(bytes)) => Some((Ok(bytes), body)),
          Ok(None) => None,
          Err(err) => Some((Err(io::Error::other(err)), body)),
        }
      }
    })
  }

  fn config(&self, upstream: &str) -> Result<S3Config, S3Error> {
    self
      .configs
      .get(upstream)
      .map(|entry| entry.clone())
      .ok_or_else(|| S3Error::UnknownUpstream(upstream.to_string()))
  }

  async fn client(&self, upstream: &str, config: &S3Config) -> Client {
    if let Some(client) = self.clients.get(upstream) {
      return client.clone();
    }

    let mut loader = aws_config::defaults(BehaviorVersion::latest())
      .region(Region::new(config.region.clone()));
    if let Some(profile) = &config.profile {
      loader = loader.profile_name(profile);
    }
    if let Some(endpoint_url) = config.endpoint_url() {
      loader = loader.endpoint_url(endpoint_url);
    }
    let shared = loader.load().await;
    let s3_config = Builder::from(&shared)
      .force_path_style(config.force_path_style())
      .build();
    let client = Client::from_conf(s3_config);
    self.clients.insert(upstream.to_string(), client.clone());
    client
  }
}

fn is_not_found<E: Display>(err: &E) -> bool {
  let text = err.to_string();
  text.contains("NotFound")
    || text.contains("NoSuchKey")
    || text.contains("status code: 404")
}
