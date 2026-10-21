mod http_client;

use std::{
  borrow::Cow,
  fmt::Write as _,
  io,
  iter,
  sync::Arc,
  time::{Duration, SystemTime},
};

use aws_config::{
  Region,
  default_provider::credentials::DefaultCredentialsChain,
  provider_config::ProviderConfig,
};
use aws_credential_types::{
  Credentials,
  provider::{ProvideCredentials, error::CredentialsError},
};
use aws_sigv4::{
  http_request::{
    PayloadChecksumKind,
    PercentEncodingMode,
    SignableBody,
    SignableRequest,
    SigningError,
    SigningSettings,
    UriPathNormalizationMode,
    sign,
  },
  sign::v4::SigningParams,
};
use aws_smithy_runtime_api::client::identity::Identity;
use bytes::Bytes;
use dashmap::DashMap;
use flub::Error;
use futures_util::{Stream, TryStreamExt};
use ncro_config::S3Config;
use reqwest::{
  Method,
  StatusCode,
  header::{
    ACCEPT_RANGES,
    CONTENT_LENGTH,
    CONTENT_RANGE,
    CONTENT_TYPE,
    ETAG,
    HeaderName,
    LAST_MODIFIED,
    RANGE,
  },
};
use tokio::{sync::Mutex, time};

use crate::http_client::ReqwestHttpClient;

/// The AWS SDK's default connect timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(3100);
/// The AWS SDK's identity cache defaults.
const CREDENTIALS_REFRESH_MARGIN: Duration = Duration::from_secs(10);
const CREDENTIALS_DEFAULT_LIFETIME: Duration = Duration::from_mins(15);
/// The AWS SDK's standard retry mode.
const MAX_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum S3Error {
  #[error("unknown S3 upstream {0:?}")]
  UnknownUpstream(String),
  #[error(
    "S3 key {key:?} has a . or .. segment, which URL parsing would collapse"
  )]
  DotSegment { key: String },
  #[error("build HTTP client for S3 upstream {upstream}")]
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
  #[error("S3 request to {url}")]
  Request {
    url:    String,
    source: reqwest::Error,
  },
  #[error("S3 returned {status} for {url}")]
  Status { url: String, status: StatusCode },
  #[error("read S3 object body from {url}")]
  Body {
    url:    String,
    source: reqwest::Error,
  },
}

pub struct S3Object {
  pub status:         u16,
  pub body:           reqwest::Response,
  pub content_type:   Option<String>,
  pub content_length: Option<u64>,
  pub content_range:  Option<String>,
  pub accept_ranges:  Option<String>,
  pub etag:           Option<String>,
  pub last_modified:  Option<String>,
}

pub struct S3ObjectHead {
  pub content_type:   Option<String>,
  pub content_length: Option<u64>,
  pub accept_ranges:  Option<String>,
  pub etag:           Option<String>,
  pub last_modified:  Option<String>,
}

#[derive(Clone, Default)]
pub struct S3ClientPool {
  configs: Arc<DashMap<String, S3Config>>,
  clients: Arc<DashMap<String, Arc<Client>>>,
}

struct Client {
  http:        reqwest::Client,
  credentials: DefaultCredentialsChain,
  cached:      Mutex<Option<(Credentials, SystemTime)>>,
}

impl Client {
  #[expect(
    clippy::significant_drop_tightening,
    reason = "holding the lock makes concurrent callers share one refresh"
  )]
  async fn credentials(&self) -> Result<Credentials, CredentialsError> {
    let mut cached = self.cached.lock().await;
    let now = SystemTime::now();
    if let Some((credentials, refresh_at)) = cached.as_ref()
      && now < *refresh_at
    {
      return Ok(credentials.clone());
    }

    let fresh = self.credentials.provide_credentials().await?;
    let refresh_at = fresh
      .expiry()
      .map_or(now + CREDENTIALS_DEFAULT_LIFETIME, |expiry| {
        expiry - CREDENTIALS_REFRESH_MARGIN
      });
    *cached = Some((fresh.clone(), refresh_at));
    Ok(fresh)
  }
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
    let Some(resp) = self.send(upstream, Method::HEAD, key, None).await? else {
      return Ok(None);
    };
    Ok(Some(S3ObjectHead {
      content_type:   header(&resp, CONTENT_TYPE),
      content_length: header(&resp, CONTENT_LENGTH)
        .and_then(|value| value.parse().ok()),
      accept_ranges:  header(&resp, ACCEPT_RANGES),
      etag:           header(&resp, ETAG),
      last_modified:  header(&resp, LAST_MODIFIED),
    }))
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
    let Some(resp) = self.send(upstream, Method::GET, key, None).await? else {
      return Ok(None);
    };
    let url = resp.url().to_string();
    let bytes = resp
      .bytes()
      .await
      .map_err(|source| S3Error::Body { url, source })?;
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
    let Some(resp) = self.send(upstream, Method::GET, key, range).await? else {
      return Ok(None);
    };
    Ok(Some(S3Object {
      status:         resp.status().as_u16(),
      content_type:   header(&resp, CONTENT_TYPE),
      content_length: header(&resp, CONTENT_LENGTH)
        .and_then(|value| value.parse().ok()),
      content_range:  header(&resp, CONTENT_RANGE),
      accept_ranges:  header(&resp, ACCEPT_RANGES),
      etag:           header(&resp, ETAG),
      last_modified:  header(&resp, LAST_MODIFIED),
      body:           resp,
    }))
  }

  pub fn body_stream(
    body: reqwest::Response,
  ) -> impl Stream<Item = Result<Bytes, io::Error>> {
    body.bytes_stream().map_err(io::Error::other)
  }

  /// Sends a signed request, mapping a missing object to `None`.
  #[expect(
    clippy::expect_used,
    reason = "SigningParams only fails to build when a field is unset"
  )]
  async fn send(
    &self,
    upstream: &str,
    method: Method,
    key: &str,
    range: Option<&str>,
  ) -> Result<Option<reqwest::Response>, S3Error> {
    let config = self.config(upstream)?;
    let client = self.client(upstream, &config).await?;
    let url = object_url(&config, key)?;

    let identity: Identity = client
      .credentials()
      .await
      .map_err(|source| {
        S3Error::Credentials {
          upstream: upstream.to_string(),
          source,
        }
      })?
      .into();
    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    let params = SigningParams::builder()
      .identity(&identity)
      .region(&config.region)
      .name("s3")
      .time(SystemTime::now())
      .settings(settings)
      .build()
      .expect("every SigV4 signing parameter is set")
      .into();
    let instructions = SignableRequest::new(
      method.as_str(),
      url.as_str(),
      iter::empty(),
      SignableBody::Bytes(&[]),
    )
    .and_then(|signable| sign(signable, &params))
    .map_err(|source| {
      S3Error::Sign {
        url: url.clone(),
        source,
      }
    })?
    .into_parts()
    .0;

    let mut attempt = 1;
    loop {
      let mut req = client.http.request(method.clone(), &url);
      for (name, value) in instructions.headers() {
        req = req.header(name, value);
      }
      if let Some(range) = range {
        req = req.header(RANGE, range);
      }

      let result = req.send().await;
      let retryable = match &result {
        Ok(resp) => {
          resp.status().is_server_error()
            || resp.status() == StatusCode::TOO_MANY_REQUESTS
        },
        Err(err) => err.is_request(),
      };
      if retryable && attempt < MAX_ATTEMPTS {
        time::sleep(RETRY_BACKOFF * attempt).await;
        attempt += 1;
        continue;
      }

      let resp = result.map_err(|source| {
        S3Error::Request {
          url: url.clone(),
          source,
        }
      })?;
      return match resp.status() {
        StatusCode::NOT_FOUND => Ok(None),
        status if status.is_success() => Ok(Some(resp)),
        status => Err(S3Error::Status { url, status }),
      };
    }
  }

  fn config(&self, upstream: &str) -> Result<S3Config, S3Error> {
    self
      .configs
      .get(upstream)
      .map(|entry| entry.clone())
      .ok_or_else(|| S3Error::UnknownUpstream(upstream.to_string()))
  }

  async fn client(
    &self,
    upstream: &str,
    config: &S3Config,
  ) -> Result<Arc<Client>, S3Error> {
    if let Some(client) = self.clients.get(upstream) {
      return Ok(Arc::clone(&client));
    }

    let http = reqwest::Client::builder()
      .connect_timeout(CONNECT_TIMEOUT)
      .build()
      .map_err(|source| {
        S3Error::HttpClient {
          upstream: upstream.to_string(),
          source,
        }
      })?;
    let provider_config =
      ProviderConfig::default().with_http_client(ReqwestHttpClient);
    let mut chain = DefaultCredentialsChain::builder()
      .configure(provider_config)
      .region(Region::new(config.region.clone()));
    if let Some(profile) = &config.profile {
      chain = chain.profile_name(profile);
    }

    let client = Arc::new(Client {
      http,
      credentials: chain.build().await,
      cached: Mutex::new(None),
    });
    self
      .clients
      .insert(upstream.to_string(), Arc::clone(&client));
    Ok(client)
  }
}

fn object_url(config: &S3Config, key: &str) -> Result<String, S3Error> {
  let key = config
    .key_prefix
    .as_ref()
    .map_or_else(|| key.to_string(), |prefix| format!("{prefix}/{key}"));
  if key
    .split('/')
    .any(|segment| segment == "." || segment == "..")
  {
    return Err(S3Error::DotSegment { key });
  }

  let mut path = String::new();
  for byte in key.bytes() {
    if byte.is_ascii_alphanumeric() || b"-._~/".contains(&byte) {
      path.push(char::from(byte));
    } else {
      let _ = write!(path, "%{byte:02X}");
    }
  }

  let endpoint = config.endpoint.as_deref().map_or_else(
    || {
      Cow::Owned(format!(
        "s3.{}.{}",
        config.region,
        dns_suffix(&config.region)
      ))
    },
    Cow::Borrowed,
  );
  if config.force_path_style() {
    Ok(format!(
      "{}://{endpoint}/{}/{path}",
      config.scheme, config.bucket
    ))
  } else {
    Ok(format!(
      "{}://{}.{endpoint}/{path}",
      config.scheme, config.bucket
    ))
  }
}

/// DNS suffixes of the non-commercial partitions in the AWS SDK's endpoint
/// rules.
fn dns_suffix(region: &str) -> &'static str {
  [
    ("cn-", "amazonaws.com.cn"),
    ("eusc-", "amazonaws.eu"),
    ("us-isob-", "sc2s.sgov.gov"),
    ("us-isof-", "csp.hci.ic.gov"),
    ("us-iso-", "c2s.ic.gov"),
    ("eu-isoe-", "cloud.adc-e.uk"),
  ]
  .into_iter()
  .find_map(|(prefix, suffix)| region.starts_with(prefix).then_some(suffix))
  .unwrap_or("amazonaws.com")
}

fn header(resp: &reqwest::Response, name: HeaderName) -> Option<String> {
  resp
    .headers()
    .get(name)
    .and_then(|value| value.to_str().ok())
    .map(str::to_owned)
}
