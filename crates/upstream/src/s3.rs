mod aws_http;

use std::{
  borrow::Cow,
  fmt::Write as _,
  iter,
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
    SigningSettings,
    UriPathNormalizationMode,
    sign,
  },
  sign::v4::SigningParams,
};
use aws_smithy_runtime_api::client::identity::Identity;
use ncro_config::S3Config;
use reqwest::{
  Method,
  StatusCode,
  header::{HeaderMap, RANGE},
};
use tokio::{
  sync::{Mutex, OnceCell},
  time,
};

use crate::{BackendError, s3::aws_http::ReqwestHttpClient};

/// The AWS SDK's default connect timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(3100);
/// The AWS SDK's identity cache defaults.
const CREDENTIALS_REFRESH_MARGIN: Duration = Duration::from_secs(10);
const CREDENTIALS_DEFAULT_LIFETIME: Duration = Duration::from_mins(15);
/// The AWS SDK's standard retry mode.
const MAX_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// An S3 bucket, reached with SigV4-signed requests over reqwest.
pub struct S3Backend {
  url:    String,
  config: S3Config,
  client: OnceCell<Client>,
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

impl S3Backend {
  pub(crate) const fn new(url: String, config: S3Config) -> Self {
    Self {
      url,
      config,
      client: OnceCell::const_new(),
    }
  }

  /// Sends a signed request for the object at `path`, mapping a missing
  /// object to `None`. Only the `range` header is forwarded.
  #[expect(
    clippy::expect_used,
    reason = "SigningParams only fails to build when a field is unset"
  )]
  pub(crate) async fn send(
    &self,
    method: Method,
    path: &str,
    headers: HeaderMap,
    timeout: Option<Duration>,
  ) -> Result<Option<reqwest::Response>, BackendError> {
    let client = self.client().await?;
    let url = object_url(&self.config, path.trim_start_matches('/'))?;

    let identity: Identity = client
      .credentials()
      .await
      .map_err(|source| {
        BackendError::Credentials {
          upstream: self.url.clone(),
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
      .region(&self.config.region)
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
      BackendError::Sign {
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
      if let Some(range) = headers.get(RANGE) {
        req = req.header(RANGE, range);
      }
      if let Some(timeout) = timeout {
        req = req.timeout(timeout);
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
        BackendError::Request {
          url: url.clone(),
          source,
        }
      })?;
      return match resp.status() {
        StatusCode::NOT_FOUND => Ok(None),
        status if status.is_success() => Ok(Some(resp)),
        status => Err(BackendError::Status { url, status }),
      };
    }
  }

  async fn client(&self) -> Result<&Client, BackendError> {
    self
      .client
      .get_or_try_init(|| {
        async {
          let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|source| {
              BackendError::HttpClient {
                upstream: self.url.clone(),
                source,
              }
            })?;
          let provider_config =
            ProviderConfig::default().with_http_client(ReqwestHttpClient);
          let mut chain = DefaultCredentialsChain::builder()
            .configure(provider_config)
            .region(Region::new(self.config.region.clone()));
          if let Some(profile) = &self.config.profile {
            chain = chain.profile_name(profile);
          }

          Ok(Client {
            http,
            credentials: chain.build().await,
            cached: Mutex::new(None),
          })
        }
      })
      .await
  }
}

fn object_url(config: &S3Config, key: &str) -> Result<String, BackendError> {
  let key = config
    .key_prefix
    .as_ref()
    .map_or_else(|| key.to_string(), |prefix| format!("{prefix}/{key}"));
  if key
    .split('/')
    .any(|segment| segment == "." || segment == "..")
  {
    return Err(BackendError::DotSegment { key });
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
