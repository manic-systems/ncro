use std::{env, fmt, fs, io, iter, time::Duration};

use netrc::Netrc;
use serde::{Deserialize, Deserializer};
use thiserror::Error;
use toml::de::Error as TomlError;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Error)]
pub enum ConfigError {
  #[error("read config: {0}")]
  Read(#[from] io::Error),
  #[error("read password_file {path:?}: {source}")]
  PasswordFile { path: String, source: io::Error },
  #[error("parse config: {0}")]
  Parse(#[from] TomlError),
  #[error("{0}")]
  Validation(String),
}

#[derive(
  Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum S3AddressingStyle {
  #[default]
  Auto,
  Path,
  Virtual,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct S3Config {
  pub bucket:           String,
  pub endpoint:         Option<String>,
  pub scheme:           String,
  pub region:           String,
  pub profile:          Option<String>,
  pub addressing_style: S3AddressingStyle,
}

impl S3Config {
  #[must_use]
  pub fn endpoint_url(&self) -> Option<String> {
    self
      .endpoint
      .as_ref()
      .map(|endpoint| format!("{}://{endpoint}", self.scheme))
  }

  #[must_use]
  pub fn force_path_style(&self) -> bool {
    match self.addressing_style {
      S3AddressingStyle::Path => true,
      S3AddressingStyle::Virtual => false,
      S3AddressingStyle::Auto => {
        self.endpoint.is_some() || self.bucket.contains('.')
      },
    }
  }
}

/// Parses a Nix-style `s3://bucket?...` URL into the settings needed by the
/// native S3 client.
///
/// Supported query parameters mirror Nix's S3 store settings that affect reads:
/// `endpoint`, `scheme`, `region`, `profile`, and `addressing-style`.
fn parse_s3_url(raw: &str) -> Result<S3Config, ConfigError> {
  let parsed = Url::parse(raw).map_err(|e| {
    ConfigError::Validation(format!("s3 upstream: invalid URL {raw:?}: {e}"))
  })?;

  let bucket = parsed
    .host_str()
    .ok_or_else(|| {
      ConfigError::Validation(format!(
        "s3 upstream {raw:?}: missing bucket name"
      ))
    })?
    .to_string();

  let mut endpoint: Option<String> = None;
  let mut scheme = "https".to_string();
  let mut region = "us-east-1".to_string();
  let mut profile: Option<String> = None;
  let mut addressing_style = S3AddressingStyle::Auto;

  for (key, value) in parsed.query_pairs() {
    match key.as_ref() {
      "endpoint" => endpoint = Some(value.into_owned()),
      "scheme" => scheme = value.into_owned(),
      "region" => region = value.into_owned(),
      "profile" => profile = Some(value.into_owned()),
      "addressing-style" => {
        addressing_style = match value.as_ref() {
          "auto" => S3AddressingStyle::Auto,
          "path" => S3AddressingStyle::Path,
          "virtual" => S3AddressingStyle::Virtual,
          other => {
            return Err(ConfigError::Validation(format!(
              "s3 upstream {raw:?}: unsupported addressing-style {other:?}"
            )));
          },
        };
      },
      _ => {},
    }
  }

  Ok(S3Config {
    bucket,
    endpoint,
    scheme,
    region,
    profile,
    addressing_style,
  })
}

/// Fill empty `username`/`password` fields from matching netrc entries.
///
/// Credentials configured explicitly always win: only upstreams with an empty
/// `username` are considered. S3 upstreams and non-HTTP(S) URLs are skipped
/// because they do not use HTTP Basic auth. An entry matches by host, and is
/// only applied when its `login` is non-empty. An empty netrc password maps to
/// `None`.
fn apply_netrc<'a>(
  upstreams: impl Iterator<Item = &'a mut UpstreamConfig>,
  nrc: &Netrc,
) {
  for upstream in upstreams {
    // Skip s3 and explicitly configured credentials
    if !upstream.username.is_empty() || upstream.s3.is_some() {
      continue;
    }

    // Skip non-HTTP(S) URLs
    let Ok(parsed) = Url::parse(&upstream.url) else {
      continue;
    };

    if !matches!(parsed.scheme(), "http" | "https") {
      continue;
    }

    let Some(host) = parsed.host_str() else {
      continue;
    };

    // Skip if no netrc entry for this host
    let Some(auth) = nrc.hosts.get(host).or_else(|| nrc.hosts.get("default"))
    else {
      continue;
    };

    // Skip if netrc entry has no login
    if auth.login.is_empty() {
      continue;
    }

    upstream.username.clone_from(&auth.login);

    if upstream.password.is_some() {
      tracing::warn!(
        "upstream {} has a password configured without a username; the \
         configured password is ignored and netrc credentials will be used \
         instead",
        upstream.url
      );
    }

    upstream.password = if auth.password.is_empty() {
      None
    } else {
      Some(auth.password.clone())
    };
  }
}

fn read_password_file(path: &str) -> Result<String, ConfigError> {
  let mut password = fs::read_to_string(path).map_err(|source| {
    ConfigError::PasswordFile {
      path: path.to_string(),
      source,
    }
  })?;
  if password.ends_with('\n') {
    password.pop();
    if password.ends_with('\r') {
      password.pop();
    }
  }
  Ok(password)
}

fn resolve_password_file(
  upstream: &mut UpstreamConfig,
  label: &str,
) -> Result<(), ConfigError> {
  let Some(path) = upstream.password_file.as_deref() else {
    return Ok(());
  };
  if upstream.password.is_some() {
    return Err(ConfigError::Validation(format!(
      "{label}: password and password_file are mutually exclusive"
    )));
  }
  upstream.password = Some(read_password_file(path)?);
  Ok(())
}

#[cfg(test)]
mod tests {
  use std::{
    iter,
    path::PathBuf,
    process,
    sync::atomic::{AtomicUsize, Ordering},
  };

  use super::*;

  static TEST_DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

  fn test_dir(name: &str) -> Result<PathBuf, ConfigError> {
    let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = env::temp_dir()
      .join(format!("ncro-config-{name}-{}-{id}", process::id()));
    fs::create_dir_all(&dir)?;
    Ok(dir)
  }

  #[test]
  fn loads_defaults() -> Result<(), ConfigError> {
    let cfg = Config::load(None)?;
    assert_eq!(cfg.server.listen, ":8080");
    assert_eq!(cfg.cache.max_entries, 100_000);
    assert_eq!(cfg.cache.slow_statement_threshold.0, Duration::from_secs(1));
    assert_eq!(cfg.upstreams.len(), 1);
    assert!(!cfg.fallback_cache.enabled);
    assert_eq!(cfg.fallback_cache.upstream.url, "https://cache.nixos.org");
    cfg.validate()?;
    Ok(())
  }

  #[test]
  fn parses_duration_toml() -> Result<(), TomlError> {
    let cfg: Config = toml::from_str(
      "[server]\ncache_priority = 40\n\n[cache]\nttl = \
       \"2h\"\nslow_statement_threshold = \"5s\"\n",
    )?;
    assert_eq!(cfg.server.cache_priority, 40);
    assert_eq!(cfg.cache.ttl.0, Duration::from_hours(2));
    assert_eq!(cfg.cache.slow_statement_threshold.0, Duration::from_secs(5));
    Ok(())
  }

  #[test]
  fn parses_upstream_filters() -> Result<(), ConfigError> {
    let cfg: Config = toml::from_str(
      "[[upstreams]]\nurl = \"https://cache.example\"\n\n\
       [[upstreams.filters]]\naction = \"allow\"\nfield = \"name\"\npattern = \"zedless*\"\n\n\
       [[upstreams.filters]]\naction = \"deny\"\nfield = \"store_path\"\npattern = \"*-source\"\n",
    )?;
    cfg.validate()?;
    assert_eq!(cfg.upstreams[0].filters.len(), 2);
    assert_eq!(cfg.upstreams[0].filters[0].action, FilterAction::Allow);
    assert_eq!(cfg.upstreams[0].filters[0].field, FilterField::Name);
    Ok(())
  }

  #[test]
  fn validates_upstream_public_keys_entries() -> Result<(), TomlError> {
    let cfg: Config = toml::from_str(
      "[[upstreams]]\nurl = \"https://cache.example\"\npublic_keys = \
       [\"missing-separator\"]\n",
    )?;

    let result = cfg.validate();

    assert!(result.is_err(), "expected validation failure");
    if let Err(err) = result {
      assert!(err.to_string().contains("public_keys[0]"));
    }
    Ok(())
  }

  #[test]
  fn logging_level_accepts_tracing_filter_directives() -> Result<(), ConfigError>
  {
    let cfg: Config =
      toml::from_str("[logging]\nlevel = \"ncro=debug,tower_http=warn\"\n")?;

    cfg.validate()?;
    Ok(())
  }

  #[test]
  fn logging_level_rejects_invalid_filter_directives() -> Result<(), TomlError>
  {
    let cfg: Config = toml::from_str("[logging]\nlevel = \"ncro==debug\"\n")?;

    let result = cfg.validate();

    assert!(result.is_err(), "expected validation failure");
    if let Err(err) = result {
      assert!(err.to_string().contains("logging.level"));
    }
    Ok(())
  }

  #[test]
  fn logging_level_rejects_empty_filter_directive() -> Result<(), TomlError> {
    let cfg: Config = toml::from_str("[logging]\nlevel = \"\"\n")?;

    let result = cfg.validate();

    assert!(result.is_err(), "expected validation failure");
    if let Err(err) = result {
      assert!(err.to_string().contains("logging.level must not be empty"));
    }
    Ok(())
  }

  #[test]
  fn logging_format_rejects_unknown_values() {
    let result = toml::from_str::<Config>("[logging]\nformat = \"pretty\"\n");

    assert!(result.is_err(), "expected parse failure");
  }

  #[test]
  fn logging_timestamps_default_to_enabled() -> Result<(), TomlError> {
    let cfg: Config = toml::from_str("[logging]\nlevel = \"info\"\n")?;

    assert!(cfg.logging.timestamps);
    Ok(())
  }

  #[test]
  fn s3_url_custom_endpoint_http() -> Result<(), ConfigError> {
    assert_eq!(
      parse_s3_url(
        "s3://my-cache?profile=default&scheme=http&region=us-east-1&\
         endpoint=minio.example.com",
      )?
      .endpoint_url()
      .as_deref(),
      Some("http://minio.example.com")
    );
    Ok(())
  }

  #[test]
  fn s3_url_custom_endpoint_default_scheme() -> Result<(), ConfigError> {
    assert_eq!(
      parse_s3_url("s3://my-cache?endpoint=minio.example.com")?
        .endpoint_url()
        .as_deref(),
      Some("https://minio.example.com")
    );
    Ok(())
  }

  #[test]
  fn s3_url_custom_endpoint_virtual_addressing() -> Result<(), ConfigError> {
    assert!(
      !parse_s3_url(
        "s3://my-cache?endpoint=minio.example.com&addressing-style=virtual"
      )?
      .force_path_style()
    );
    Ok(())
  }

  #[test]
  fn s3_url_aws_path_addressing() -> Result<(), ConfigError> {
    assert!(
      parse_s3_url("s3://my-cache?region=eu-west-1&addressing-style=path")?
        .force_path_style()
    );
    Ok(())
  }

  #[test]
  fn s3_url_aws_auto_uses_path_for_dotted_bucket() -> Result<(), ConfigError> {
    assert!(parse_s3_url("s3://my.cache?region=eu-west-1")?.force_path_style());
    Ok(())
  }

  #[test]
  fn s3_url_rejects_unknown_addressing_style() {
    assert!(
      parse_s3_url("s3://my-cache?addressing-style=unsupported").is_err()
    );
  }

  #[test]
  fn s3_url_aws_with_region() -> Result<(), ConfigError> {
    assert_eq!(
      parse_s3_url("s3://my-cache?region=eu-west-1")?.region,
      "eu-west-1"
    );
    Ok(())
  }

  #[test]
  fn s3_url_aws_no_params() -> Result<(), ConfigError> {
    assert_eq!(parse_s3_url("s3://my-cache")?.region, "us-east-1");
    Ok(())
  }

  #[test]
  fn s3_url_missing_bucket_is_error() {
    assert!(parse_s3_url("s3://").is_err());
  }

  #[test]
  fn load_parses_s3_upstream() -> Result<(), ConfigError> {
    let toml = r#"
[[upstreams]]
url = "s3://dcr-nix-cache?profile=default&scheme=http&region=us-east-1&endpoint=s3.example.com"
priority = 10
"#;
    let cfg: Config = toml::from_str(toml)?;
    let s3 = parse_s3_url(&cfg.upstreams[0].url)?;
    assert_eq!(s3.endpoint_url().as_deref(), Some("http://s3.example.com"));
    Ok(())
  }

  #[test]
  fn load_parses_s3_fallback_cache() -> Result<(), ConfigError> {
    let toml = r#"
[[upstreams]]
url = "https://cache.example"

[fallback_cache]
enabled = true
url = "s3://fallback-cache?endpoint=s3.example.com"
"#;
    let cfg: Config = toml::from_str(toml)?;
    let mut cfg = cfg;
    if cfg.fallback_cache.upstream.url.starts_with("s3://") {
      cfg.fallback_cache.upstream.s3 =
        Some(parse_s3_url(&cfg.fallback_cache.upstream.url)?);
    }
    assert!(cfg.fallback_cache.enabled);
    assert!(cfg.fallback_cache.upstream.s3.is_some());
    Ok(())
  }

  #[test]
  #[expect(clippy::expect_used)]
  fn empty_listen_passes_validation() {
    // Socket activation supplies the fd at runtime; an empty listen address
    // must not be rejected at config validation time.
    let toml = "[[upstreams]]\nurl = \"https://cache.nixos.org\"\npriority = \
                1\n[server]\nlisten = \"\"\n";
    let cfg: Config = toml::from_str(toml).expect("parse");
    assert!(cfg.validate().is_ok());
  }

  #[test]
  fn validates_mass_query_limits() -> Result<(), TomlError> {
    let cfg: Config = toml::from_str(
      "[cache.mass_query]\nmax_concurrent_races = \
       0\nper_upstream_max_inflight = 1\nin_memory_negative_ttl = \
       \"5s\"\nupstream_cooldown = \"10s\"\n",
    )?;
    let result = cfg.validate();
    assert!(result.is_err(), "expected validation failure");
    if let Err(err) = result {
      assert!(
        err
          .to_string()
          .contains("cache.mass_query.max_concurrent_races must be >= 1")
      );
    }
    Ok(())
  }

  #[expect(clippy::expect_used)]
  fn netrc(content: &str) -> Netrc {
    use std::str::FromStr as _;
    Netrc::from_str(content).expect("valid netrc")
  }

  fn upstream(url: &str) -> UpstreamConfig {
    UpstreamConfig {
      url: url.to_string(),
      ..Default::default()
    }
  }

  #[test]
  fn netrc_fills_empty_credentials() {
    let nrc = netrc("machine cache.example.com login alice password secret\n");
    let mut up = upstream("https://cache.example.com");
    apply_netrc(iter::once(&mut up), &nrc);
    assert_eq!(up.username, "alice");
    assert_eq!(up.password.as_deref(), Some("secret"));
  }

  #[test]
  fn netrc_does_not_override_config_credentials() {
    let nrc = netrc("machine cache.example.com login alice password secret\n");
    let mut up = upstream("https://cache.example.com");
    up.username = "bob".to_string();
    up.password = Some("configpw".to_string());
    apply_netrc(iter::once(&mut up), &nrc);
    assert_eq!(up.username, "bob");
    assert_eq!(up.password.as_deref(), Some("configpw"));
  }

  #[test]
  fn netrc_ignores_non_matching_host() {
    let nrc = netrc("machine other.example.com login alice password secret\n");
    let mut up = upstream("https://cache.example.com");
    apply_netrc(iter::once(&mut up), &nrc);
    assert!(up.username.is_empty());
    assert!(up.password.is_none());
  }

  #[test]
  #[expect(clippy::expect_used)]
  fn netrc_skips_s3_upstreams() {
    let nrc = netrc("machine my-cache login alice password secret\n");
    let mut up = upstream("s3://my-cache?endpoint=s3.example.com");
    up.s3 = Some(parse_s3_url(&up.url).expect("valid s3 url"));
    apply_netrc(iter::once(&mut up), &nrc);
    assert!(up.username.is_empty());
    assert!(up.password.is_none());
  }

  #[test]
  fn netrc_empty_password_maps_to_none() {
    let nrc = netrc("machine cache.example.com login alice\n");
    let mut up = upstream("https://cache.example.com");
    apply_netrc(iter::once(&mut up), &nrc);
    assert_eq!(up.username, "alice");
    assert!(up.password.is_none());
  }

  #[test]
  fn netrc_default_entry_matches_any_host() {
    let nrc = netrc("default login alice password secret\n");
    let mut up = upstream("https://cache.example.com");
    apply_netrc(iter::once(&mut up), &nrc);
    assert_eq!(up.username, "alice");
    assert_eq!(up.password.as_deref(), Some("secret"));
  }

  #[test]
  fn netrc_explicit_machine_takes_precedence_over_default() {
    let nrc = netrc(
      "machine cache.example.com login alice password secret\ndefault login \
       bob password fallback\n",
    );
    let mut up = upstream("https://cache.example.com");
    apply_netrc(iter::once(&mut up), &nrc);
    assert_eq!(up.username, "alice");
    assert_eq!(up.password.as_deref(), Some("secret"));
  }

  #[test]
  fn password_file_fills_upstream_password() -> Result<(), ConfigError> {
    let dir = test_dir("password-file-fills")?;
    let password_path = dir.join("password");
    let config_path = dir.join("ncro.toml");
    fs::write(&password_path, "secret\n")?;
    fs::write(
      &config_path,
      format!(
        "[[upstreams]]\nurl = \"https://cache.example.com\"\nusername = \
         \"alice\"\npassword_file = {:?}\n",
        password_path
      ),
    )?;

    let config_path = config_path.to_string_lossy();
    let cfg = Config::load(Some(&config_path))?;

    assert_eq!(cfg.upstreams[0].password.as_deref(), Some("secret"));
    Ok(())
  }

  #[test]
  fn password_file_preserves_non_newline_whitespace() -> Result<(), ConfigError>
  {
    let dir = test_dir("password-file-whitespace")?;
    let password_path = dir.join("password");
    let config_path = dir.join("ncro.toml");
    fs::write(&password_path, " secret \n")?;
    fs::write(
      &config_path,
      format!(
        "[[upstreams]]\nurl = \"https://cache.example.com\"\nusername = \
         \"alice\"\npassword_file = {:?}\n",
        password_path
      ),
    )?;

    let config_path = config_path.to_string_lossy();
    let cfg = Config::load(Some(&config_path))?;

    assert_eq!(cfg.upstreams[0].password.as_deref(), Some(" secret "));
    Ok(())
  }

  #[test]
  fn password_and_password_file_are_mutually_exclusive() -> Result<(), TomlError>
  {
    let cfg: Config = toml::from_str(
      "[[upstreams]]\nurl = \"https://cache.example.com\"\nusername = \
       \"alice\"\npassword = \"inline\"\npassword_file = \"/run/secrets/pw\"\n",
    )?;

    let result = cfg.validate();

    assert!(result.is_err(), "expected validation failure");
    if let Err(err) = result {
      assert!(
        err
          .to_string()
          .contains("password and password_file are mutually exclusive")
      );
    }
    Ok(())
  }

  #[test]
  fn upstream_debug_redacts_password() {
    let mut upstream = upstream("https://cache.example.com");
    upstream.password = Some("secret".to_string());

    let debug = format!("{upstream:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("secret"));
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

impl Default for HumanDuration {
  fn default() -> Self {
    Self(Duration::ZERO)
  }
}

impl<'de> Deserialize<'de> for HumanDuration {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: Deserializer<'de>,
  {
    humantime_serde::deserialize(deserializer).map(Self)
  }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FilterAction {
  #[default]
  Allow,
  Deny,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FilterField {
  #[default]
  Name,
  StorePath,
  Reference,
  Deriver,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NarUrlMode {
  /// Return the upstream narinfo `URL:` line unchanged.
  #[default]
  Keep,
  /// Rewrite `URL:` to a relative path so Nix fetches NARs through NCRO.
  ToSelf,
  /// Rewrite `URL:` to an absolute URL rooted at the selected upstream.
  ToUpstream,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FilterRule {
  pub action:  FilterAction,
  pub field:   FilterField,
  pub pattern: String,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
  /// Base URL of the substituter (http, https, or `s3://`).
  pub url:             String,
  /// Priority for latency tiebreaking and race ordering. Lower values are
  /// preferred. Two upstreams within 10 % EMA latency are resolved by
  /// this priority.
  pub priority:        i32,
  /// Nix-style `name:base64(key)` public key for narinfo signature
  /// verification. Leave empty to skip verification unless `public_keys` is
  /// set.
  pub public_key:      String,
  /// Additional Nix-style public keys accepted for narinfo signature
  /// verification. Useful for pull-through caches that may serve narinfos
  /// signed by an origin cache key.
  pub public_keys:     Vec<String>,
  /// HTTP Basic Auth username. Requests are made without authentication
  /// when empty.
  pub username:        String,
  /// HTTP Basic Auth password.
  pub password:        Option<String>,
  /// Path to a file containing the HTTP Basic Auth password.
  pub password_file:   Option<String>,
  /// Per-upstream allow/deny path filters evaluated after the narinfo is
  /// fetched. Deny rules reject immediately; if any allow rules exist,
  /// at least one must match.
  pub filters:         Vec<FilterRule>,
  /// Controls how the narinfo `URL:` field is returned to clients.
  pub nar_url_mode:    NarUrlMode,
  /// Optional per-upstream timeout for narinfo HEAD races and GET fetches.
  /// When unset, the router's global race timeout is used.
  pub narinfo_timeout: Option<HumanDuration>,
  /// Optional per-upstream timeout for NAR file streaming requests.
  /// When unset, the server's global `read_timeout` is used.
  pub nar_timeout:     Option<HumanDuration>,
  /// Parsed S3 configuration derived from `url` when the scheme is `s3://`.
  /// Not set directly in config, but populated automatically at load time.
  #[serde(skip)]
  pub s3:              Option<S3Config>,
}

impl fmt::Debug for UpstreamConfig {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("UpstreamConfig")
      .field("url", &self.url)
      .field("priority", &self.priority)
      .field("public_key", &self.public_key)
      .field("public_keys", &self.public_keys)
      .field("username", &self.username)
      .field("password", &self.password.as_ref().map(|_| "<redacted>"))
      .field("password_file", &self.password_file)
      .field("filters", &self.filters)
      .field("nar_url_mode", &self.nar_url_mode)
      .field("narinfo_timeout", &self.narinfo_timeout)
      .field("nar_timeout", &self.nar_timeout)
      .field("s3", &self.s3)
      .finish()
  }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FallbackCacheConfig {
  pub enabled:  bool,
  #[serde(flatten)]
  pub upstream: UpstreamConfig,
}

impl Default for FallbackCacheConfig {
  fn default() -> Self {
    Self {
      enabled:  false,
      upstream: UpstreamConfig {
        url: "https://cache.nixos.org".to_string(),
        public_key: "cache.nixos.org-1:\
                     6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
          .to_string(),
        ..Default::default()
      },
    }
  }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
  pub listen:         String,
  pub cache_priority: i32,
  pub read_timeout:   HumanDuration,
  pub write_timeout:  HumanDuration,
}

impl Default for ServerConfig {
  fn default() -> Self {
    Self {
      listen:         ":8080".to_string(),
      cache_priority: 30,
      read_timeout:   HumanDuration(Duration::from_secs(30)),
      write_timeout:  HumanDuration(Duration::from_secs(30)),
    }
  }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
  pub db_path:                  String,
  pub max_entries:              i64,
  pub ttl:                      HumanDuration,
  pub negative_ttl:             HumanDuration,
  pub latency_alpha:            f64,
  pub slow_statement_threshold: HumanDuration,
  pub mass_query:               MassQueryConfig,
}

impl Default for CacheConfig {
  fn default() -> Self {
    Self {
      db_path:                  "/var/lib/ncro/routes.db".to_string(),
      max_entries:              100_000,
      ttl:                      HumanDuration(Duration::from_hours(1)),
      negative_ttl:             HumanDuration(Duration::from_mins(10)),
      latency_alpha:            0.3,
      slow_statement_threshold: HumanDuration(Duration::from_secs(1)),
      mass_query:               MassQueryConfig::default(),
    }
  }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MassQueryConfig {
  pub max_concurrent_races:      u32,
  pub per_upstream_max_inflight: u32,
  pub in_memory_negative_ttl:    HumanDuration,
  pub upstream_cooldown:         HumanDuration,
}

impl Default for MassQueryConfig {
  fn default() -> Self {
    Self {
      max_concurrent_races:      64,
      per_upstream_max_inflight: 8,
      in_memory_negative_ttl:    HumanDuration(Duration::from_secs(5)),
      upstream_cooldown:         HumanDuration(Duration::from_secs(15)),
    }
  }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PeerConfig {
  pub addr:       String,
  pub public_key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MeshConfig {
  pub enabled:          bool,
  pub bind_addr:        String,
  pub peers:            Vec<PeerConfig>,
  #[serde(rename = "private_key")]
  pub private_key_path: String,
  pub gossip_interval:  HumanDuration,
}

impl Default for MeshConfig {
  fn default() -> Self {
    Self {
      enabled:          false,
      bind_addr:        "0.0.0.0:7946".to_string(),
      peers:            Vec::new(),
      private_key_path: String::new(),
      gossip_interval:  HumanDuration(Duration::from_secs(30)),
    }
  }
}

/// Which address families to use when registering mDNS-discovered peers.
///
/// A discovered service may advertise multiple addresses (IPv4 and IPv6).
/// This option controls which are registered as upstreams.  `any` (default)
/// registers all routable addresses and lets the router race them; set `ipv4`
/// or `ipv6` to restrict to a single family when the upstream server is known
/// to only listen on one (e.g. nix-serve binds to 0.0.0.0 by default).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AddressFamily {
  #[default]
  Any,
  Ipv4,
  Ipv6,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
  pub enabled:        bool,
  pub service_name:   String,
  pub domain:         String,
  pub discovery_time: HumanDuration,
  pub priority:       i32,
  pub address_family: AddressFamily,
}

impl Default for DiscoveryConfig {
  fn default() -> Self {
    Self {
      enabled:        false,
      service_name:   "_nix-serve._tcp".to_string(),
      domain:         "local".to_string(),
      discovery_time: HumanDuration(Duration::from_secs(5)),
      priority:       20,
      address_family: AddressFamily::default(),
    }
  }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
  pub level:      String,
  pub format:     LogFormat,
  pub timestamps: bool,
}

impl Default for LoggingConfig {
  fn default() -> Self {
    Self {
      level:      "info".to_string(),
      format:     LogFormat::default(),
      timestamps: true,
    }
  }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
  #[default]
  Json,
  Text,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
  pub server:         ServerConfig,
  pub upstreams:      Vec<UpstreamConfig>,
  pub fallback_cache: FallbackCacheConfig,
  pub cache:          CacheConfig,
  pub mesh:           MeshConfig,
  pub discovery:      DiscoveryConfig,
  pub logging:        LoggingConfig,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      server:         ServerConfig::default(),
      upstreams:      vec![UpstreamConfig {
        url: "https://cache.nixos.org".to_string(),
        priority: 10,
        public_key: String::new(),
        ..Default::default()
      }],
      fallback_cache: FallbackCacheConfig::default(),
      cache:          CacheConfig::default(),
      mesh:           MeshConfig::default(),
      discovery:      DiscoveryConfig::default(),
      logging:        LoggingConfig::default(),
    }
  }
}

impl Config {
  /// # Errors
  ///
  /// Returns [`ConfigError::Read`] if the config file cannot be read, or
  /// [`ConfigError::Parse`] if the TOML is malformed.
  pub fn load(path: Option<&str>) -> Result<Self, ConfigError> {
    let mut cfg = if let Some(path) = path.filter(|p| !p.is_empty()) {
      let data = fs::read_to_string(path)?;
      toml::from_str::<Self>(&data)?
    } else {
      Self::default()
    };

    if let Ok(v) = env::var("NCRO_LISTEN")
      && !v.is_empty()
    {
      cfg.server.listen = v;
    }
    if let Ok(v) = env::var("NCRO_DB_PATH")
      && !v.is_empty()
    {
      cfg.cache.db_path = v;
    }
    if let Ok(v) = env::var("NCRO_LOG_LEVEL")
      && !v.is_empty()
    {
      cfg.logging.level = v;
    }

    for upstream in &mut cfg.upstreams {
      if upstream.url.starts_with("s3://") {
        upstream.s3 = Some(parse_s3_url(&upstream.url)?);
      }
    }
    if cfg.fallback_cache.upstream.url.starts_with("s3://") {
      cfg.fallback_cache.upstream.s3 =
        Some(parse_s3_url(&cfg.fallback_cache.upstream.url)?);
    }

    for (i, upstream) in cfg.upstreams.iter_mut().enumerate() {
      resolve_password_file(upstream, &format!("upstream[{i}]"))?;
    }
    if cfg.fallback_cache.enabled {
      resolve_password_file(
        &mut cfg.fallback_cache.upstream,
        "fallback_cache",
      )?;
    }

    if let Ok(nrc) = Netrc::new() {
      apply_netrc(
        cfg
          .upstreams
          .iter_mut()
          .chain(iter::once(&mut cfg.fallback_cache.upstream)),
        &nrc,
      );
    }

    Ok(cfg)
  }

  /// # Errors
  ///
  /// Returns [`ConfigError::Validation`] if any field fails the constraint
  /// checks (e.g. empty upstream list, out-of-range latency alpha).
  pub fn validate(&self) -> Result<(), ConfigError> {
    if self.upstreams.is_empty() {
      return Err(ConfigError::Validation(
        "at least one upstream is required".to_string(),
      ));
    }
    for (i, upstream) in self.upstreams.iter().enumerate() {
      validate_upstream(upstream, &format!("upstream[{i}]"))?;
    }
    if self.fallback_cache.enabled {
      validate_upstream(&self.fallback_cache.upstream, "fallback_cache")?;
    }
    if self.server.cache_priority < 1 {
      return Err(ConfigError::Validation(format!(
        "server.cache_priority must be >= 1, got {}",
        self.server.cache_priority
      )));
    }
    if self.server.read_timeout.0.is_zero() {
      return Err(ConfigError::Validation(
        "server.read_timeout must be positive".to_string(),
      ));
    }
    if self.server.write_timeout.0.is_zero() {
      return Err(ConfigError::Validation(
        "server.write_timeout must be positive".to_string(),
      ));
    }
    if self.cache.latency_alpha <= 0.0 || self.cache.latency_alpha >= 1.0 {
      return Err(ConfigError::Validation(format!(
        "cache.latency_alpha must be between 0 and 1 exclusive, got {}",
        self.cache.latency_alpha
      )));
    }
    if self.cache.ttl.0.is_zero() {
      return Err(ConfigError::Validation(
        "cache.ttl must be positive".to_string(),
      ));
    }
    if self.cache.negative_ttl.0.is_zero() {
      return Err(ConfigError::Validation(
        "cache.negative_ttl must be positive".to_string(),
      ));
    }
    if self.cache.max_entries <= 0 {
      return Err(ConfigError::Validation(
        "cache.max_entries must be positive".to_string(),
      ));
    }
    if self.cache.mass_query.max_concurrent_races == 0 {
      return Err(ConfigError::Validation(
        "cache.mass_query.max_concurrent_races must be >= 1".to_string(),
      ));
    }
    if self.cache.mass_query.per_upstream_max_inflight == 0 {
      return Err(ConfigError::Validation(
        "cache.mass_query.per_upstream_max_inflight must be >= 1".to_string(),
      ));
    }
    if self.cache.mass_query.in_memory_negative_ttl.0.is_zero() {
      return Err(ConfigError::Validation(
        "cache.mass_query.in_memory_negative_ttl must be positive".to_string(),
      ));
    }
    if self.cache.mass_query.upstream_cooldown.0.is_zero() {
      return Err(ConfigError::Validation(
        "cache.mass_query.upstream_cooldown must be positive".to_string(),
      ));
    }
    if self.logging.level.trim().is_empty() {
      return Err(ConfigError::Validation(
        "logging.level must not be empty".to_string(),
      ));
    }
    EnvFilter::try_new(&self.logging.level).map_err(|err| {
      ConfigError::Validation(format!(
        "logging.level must be a valid tracing filter directive, got {:?}: \
         {err}",
        self.logging.level
      ))
    })?;
    if self.mesh.enabled && self.mesh.peers.is_empty() {
      return Err(ConfigError::Validation(
        "mesh.enabled is true but no peers configured".to_string(),
      ));
    }
    for (i, peer) in self.mesh.peers.iter().enumerate() {
      if peer.addr.is_empty() {
        return Err(ConfigError::Validation(format!(
          "mesh.peers[{i}]: addr is empty"
        )));
      }
      if !peer.public_key.is_empty() {
        let bytes = hex::decode(&peer.public_key).map_err(|_| {
          ConfigError::Validation(format!(
            "mesh.peers[{i}]: public_key must be a hex-encoded 32-byte \
             ed25519 key"
          ))
        })?;
        if bytes.len() != 32 {
          return Err(ConfigError::Validation(format!(
            "mesh.peers[{i}]: public_key must be a hex-encoded 32-byte \
             ed25519 key"
          )));
        }
      }
    }
    if self.discovery.enabled {
      if self.discovery.service_name.is_empty() {
        return Err(ConfigError::Validation(
          "discovery.service_name is required when discovery is enabled"
            .to_string(),
        ));
      }
      if self.discovery.domain.is_empty() {
        return Err(ConfigError::Validation(
          "discovery.domain is required when discovery is enabled".to_string(),
        ));
      }
      if self.discovery.discovery_time.0.is_zero() {
        return Err(ConfigError::Validation(
          "discovery.discovery_time must be positive".to_string(),
        ));
      }
    }
    Ok(())
  }
}

fn validate_upstream(
  upstream: &UpstreamConfig,
  label: &str,
) -> Result<(), ConfigError> {
  if upstream.url.is_empty() {
    return Err(ConfigError::Validation(format!("{label}: URL is empty")));
  }
  Url::parse(&upstream.url).map_err(|err| {
    ConfigError::Validation(format!(
      "{label}: invalid URL {:?}: {err}",
      upstream.url
    ))
  })?;
  if !upstream.public_key.is_empty() {
    validate_nix_public_key(
      &upstream.public_key,
      &format!("{label}: public_key"),
    )?;
  }
  for (i, public_key) in upstream.public_keys.iter().enumerate() {
    validate_nix_public_key(public_key, &format!("{label}: public_keys[{i}]"))?;
  }
  if upstream.password.is_some() && upstream.password_file.is_some() {
    return Err(ConfigError::Validation(format!(
      "{label}: password and password_file are mutually exclusive"
    )));
  }
  for (j, filter) in upstream.filters.iter().enumerate() {
    if filter.pattern.is_empty() {
      return Err(ConfigError::Validation(format!(
        "{label}.filters[{j}]: pattern is empty"
      )));
    }
  }
  Ok(())
}

fn validate_nix_public_key(
  public_key: &str,
  label: &str,
) -> Result<(), ConfigError> {
  if public_key.is_empty() || !public_key.contains(':') {
    return Err(ConfigError::Validation(format!(
      "{label} must be in 'name:base64(key)' Nix format"
    )));
  }
  Ok(())
}
