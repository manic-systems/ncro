use std::{
  io,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use sqlx::{
  ConnectOptions,
  Row,
  SqlitePool,
  sqlite::{
    SqliteConnectOptions,
    SqliteJournalMode,
    SqlitePoolOptions,
    SqliteRow,
  },
};
use thiserror::Error;
use tokio::{fs as tokio_fs, sync::Mutex};

#[derive(Debug, Error)]
pub enum DbError {
  #[error("sqlite: {0}")]
  Sqlx(#[from] sqlx::Error),
  #[error("create database directory: {0}")]
  CreateDir(#[from] io::Error),
  #[error("invalid stored route data: {0}")]
  InvalidData(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RouteEntry {
  pub store_path:       String,
  pub upstream_url:     String,
  pub latency_ms:       f64,
  pub latency_ema:      f64,
  pub last_verified:    DateTime<Utc>,
  pub query_count:      u32,
  pub failure_count:    u32,
  pub ttl:              DateTime<Utc>,
  pub nar_hash:         String,
  pub nar_size:         u64,
  pub nar_url:          String,
  /// The upstream's own path, since `nar_url` is canonical.
  pub upstream_nar_url: String,
  pub narinfo_bytes:    Option<Vec<u8>>,
}

impl RouteEntry {
  #[must_use]
  pub fn is_valid(&self) -> bool {
    Utc::now() < self.ttl
  }
}

#[derive(Debug, Clone)]
pub struct HealthRow {
  pub url:               String,
  pub ema_latency:       f64,
  pub consecutive_fails: i64,
  pub total_queries:     i64,
}

#[derive(Clone)]
pub struct Db {
  pool:        SqlitePool,
  max_entries: i64,
  write_count: Arc<AtomicU64>,
  write_lock:  Arc<Mutex<()>>,
}

impl Db {
  /// # Errors
  ///
  /// Returns [`DbError`] if the database directory cannot be created, the
  /// connection pool fails to open, or the schema migration fails.
  pub async fn open(
    path: &str,
    max_entries: i64,
    slow_statement_threshold: Duration,
  ) -> Result<Self, DbError> {
    if path != ":memory:"
      && let Some(parent) = Path::new(path).parent()
    {
      tokio_fs::create_dir_all(parent).await?;
    }

    let options = if path == ":memory:" {
      SqliteConnectOptions::new().filename(path)
    } else {
      SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
    }
    .journal_mode(SqliteJournalMode::Wal)
    .busy_timeout(Duration::from_secs(5))
    .log_slow_statements(log::LevelFilter::Warn, slow_statement_threshold);

    // In-memory databases are per-connection; use a single connection so all
    // operations share the same database. File-based WAL allows concurrent
    // readers.
    let max_conn = if path == ":memory:" { 1 } else { 8 };
    let pool = SqlitePoolOptions::new()
      .max_connections(max_conn)
      .connect_with(options)
      .await?;
    migrate(&pool).await?;
    Ok(Self {
      pool,
      max_entries,
      write_count: Arc::new(AtomicU64::new(0)),
      write_lock: Arc::new(Mutex::new(())),
    })
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` query failure or invalid stored data.
  pub async fn get_route(
    &self,
    store_path: &str,
  ) -> Result<Option<RouteEntry>, DbError> {
    let row = sqlx::query(
            r"SELECT store_path, upstream_url, latency_ms, latency_ema, query_count, failure_count,
                      last_verified, ttl, nar_hash, nar_size, nar_url, upstream_nar_url, narinfo_bytes
                 FROM routes WHERE store_path = ?",
        )
        .bind(store_path)
        .fetch_optional(&self.pool)
        .await?;
    row.as_ref().map(row_to_route).transpose()
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` query failure or invalid stored data.
  pub async fn get_route_by_nar_url(
    &self,
    nar_url: &str,
  ) -> Result<Option<RouteEntry>, DbError> {
    let row = sqlx::query(
            r"SELECT store_path, upstream_url, latency_ms, latency_ema, query_count, failure_count,
                      last_verified, ttl, nar_hash, nar_size, nar_url, upstream_nar_url, narinfo_bytes
                 FROM routes WHERE nar_url = ? AND ttl > ?",
        )
        .bind(nar_url)
        .bind(Utc::now().timestamp())
        .fetch_optional(&self.pool)
        .await?;
    row.as_ref().map(row_to_route).transpose()
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` write failure.
  pub async fn set_route(&self, entry: &RouteEntry) -> Result<(), DbError> {
    let _guard = self.write_lock.lock().await;
    sqlx::query(
            r"INSERT INTO routes
               (store_path, upstream_url, latency_ms, latency_ema, query_count, failure_count,
                last_verified, ttl, nar_hash, nar_size, nar_url, upstream_nar_url, narinfo_bytes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(store_path) DO UPDATE SET
                 upstream_url = excluded.upstream_url,
                 latency_ms = excluded.latency_ms,
                 latency_ema = excluded.latency_ema,
                 query_count = excluded.query_count,
                 failure_count = excluded.failure_count,
                 last_verified = excluded.last_verified,
                 ttl = excluded.ttl,
                 nar_hash = excluded.nar_hash,
                 nar_size = excluded.nar_size,
                 nar_url = excluded.nar_url,
                 upstream_nar_url = excluded.upstream_nar_url,
                 narinfo_bytes = excluded.narinfo_bytes",
        )
        .bind(&entry.store_path)
        .bind(&entry.upstream_url)
        .bind(entry.latency_ms)
        .bind(entry.latency_ema)
        .bind(i64::from(entry.query_count))
        .bind(i64::from(entry.failure_count))
        .bind(entry.last_verified.timestamp())
        .bind(entry.ttl.timestamp())
        .bind(&entry.nar_hash)
        .bind(i64::try_from(entry.nar_size).unwrap_or(i64::MAX))
        .bind(&entry.nar_url)
        .bind(&entry.upstream_nar_url)
        .bind(&entry.narinfo_bytes)
        .execute(&self.pool)
        .await?;
    let count = self.write_count.fetch_add(1, Ordering::Relaxed);
    if count % 100 == 99 {
      self.evict_if_needed().await?;
    }
    Ok(())
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` write failure.
  pub async fn expire_old_routes(&self) -> Result<(), DbError> {
    let _guard = self.write_lock.lock().await;
    sqlx::query("DELETE FROM routes WHERE ttl < ?")
      .bind(Utc::now().timestamp())
      .execute(&self.pool)
      .await?;
    Ok(())
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` query failure or invalid stored data.
  pub async fn list_recent_routes(
    &self,
    n: i64,
  ) -> Result<Vec<RouteEntry>, DbError> {
    let rows = sqlx::query(
            r"SELECT store_path, upstream_url, latency_ms, latency_ema, query_count, failure_count,
                      last_verified, ttl, nar_hash, nar_size, nar_url, upstream_nar_url, narinfo_bytes
                 FROM routes WHERE ttl > ? ORDER BY last_verified DESC LIMIT ?",
        )
        .bind(Utc::now().timestamp())
        .bind(n)
        .fetch_all(&self.pool)
        .await?;
    rows.iter().map(row_to_route).collect()
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` query failure.
  pub async fn route_count(&self) -> Result<i64, DbError> {
    Ok(
      sqlx::query("SELECT COUNT(*) FROM routes")
        .fetch_one(&self.pool)
        .await?
        .get::<i64, _>(0),
    )
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` write failure.
  pub async fn set_negative(
    &self,
    store_path: &str,
    ttl: Duration,
  ) -> Result<(), DbError> {
    let _guard = self.write_lock.lock().await;
    sqlx::query(
            r"INSERT INTO negative_cache (store_path, expires_at) VALUES (?, ?)
               ON CONFLICT(store_path) DO UPDATE SET expires_at = excluded.expires_at",
        )
        .bind(store_path)
        .bind((Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default()).timestamp())
        .execute(&self.pool)
        .await?;
    Ok(())
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` query failure.
  pub async fn is_negative(&self, store_path: &str) -> Result<bool, DbError> {
    Ok(
      sqlx::query(
        "SELECT EXISTS(SELECT 1 FROM negative_cache WHERE store_path = ? AND \
         expires_at > ?)",
      )
      .bind(store_path)
      .bind(Utc::now().timestamp())
      .fetch_one(&self.pool)
      .await?
      .get::<i64, _>(0)
        != 0,
    )
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` write failure.
  pub async fn expire_negatives(&self) -> Result<(), DbError> {
    let _guard = self.write_lock.lock().await;
    sqlx::query("DELETE FROM negative_cache WHERE expires_at < ?")
      .bind(Utc::now().timestamp())
      .execute(&self.pool)
      .await?;
    Ok(())
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` write failure.
  pub async fn save_health(
    &self,
    url: &str,
    ema: f64,
    consecutive_fails: i64,
    total_queries: i64,
  ) -> Result<(), DbError> {
    let _guard = self.write_lock.lock().await;
    sqlx::query(
            r"INSERT INTO upstream_health (url, ema_latency, consecutive_fails, total_queries)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(url) DO UPDATE SET
                 ema_latency = excluded.ema_latency,
                 consecutive_fails = excluded.consecutive_fails,
                 total_queries = excluded.total_queries",
        )
        .bind(url)
        .bind(ema)
        .bind(consecutive_fails)
        .bind(total_queries)
        .execute(&self.pool)
        .await?;
    Ok(())
  }

  /// # Errors
  ///
  /// Returns [`DbError`] on `SQLite` query failure.
  pub async fn load_all_health(&self) -> Result<Vec<HealthRow>, DbError> {
    let rows = sqlx::query(
      "SELECT url, ema_latency, consecutive_fails, total_queries FROM \
       upstream_health",
    )
    .fetch_all(&self.pool)
    .await?;
    Ok(
      rows
        .into_iter()
        .map(|row| {
          HealthRow {
            url:               row.get("url"),
            ema_latency:       row.get("ema_latency"),
            consecutive_fails: row.get("consecutive_fails"),
            total_queries:     row.get("total_queries"),
          }
        })
        .collect(),
    )
  }

  async fn evict_if_needed(&self) -> Result<(), DbError> {
    sqlx::query(
      r"DELETE FROM routes WHERE store_path IN (
                 SELECT store_path FROM routes ORDER BY last_verified ASC
                 LIMIT MAX(0, (SELECT COUNT(*) FROM routes) - ?)
               )",
    )
    .bind(self.max_entries)
    .execute(&self.pool)
    .await?;
    Ok(())
  }
}

async fn add_column_if_missing(
  pool: &SqlitePool,
  table: &str,
  column: &str,
  definition: &str,
) -> Result<(), DbError> {
  let rows =
    sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info({table})")))
      .fetch_all(pool)
      .await?;
  let exists = rows.iter().any(|row| {
    let name: String = row.get("name");
    name == column
  });
  if !exists {
    sqlx::query(sqlx::AssertSqlSafe(format!(
      "ALTER TABLE {table} ADD COLUMN {column} {definition}"
    )))
    .execute(pool)
    .await?;
  }
  Ok(())
}

async fn migrate(pool: &SqlitePool) -> Result<(), DbError> {
  sqlx::query(
    r"CREATE TABLE IF NOT EXISTS routes (
             store_path TEXT PRIMARY KEY,
             upstream_url TEXT NOT NULL,
             latency_ms REAL NOT NULL DEFAULT 0,
             latency_ema REAL NOT NULL DEFAULT 0,
             query_count INTEGER NOT NULL DEFAULT 1,
             failure_count INTEGER NOT NULL DEFAULT 0,
             last_verified INTEGER NOT NULL DEFAULT 0,
             ttl INTEGER NOT NULL,
             nar_hash TEXT NOT NULL DEFAULT '',
             nar_size INTEGER NOT NULL DEFAULT 0,
             nar_url TEXT NOT NULL DEFAULT '',
             created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
           )",
  )
  .execute(pool)
  .await?;
  sqlx::query("CREATE INDEX IF NOT EXISTS idx_routes_ttl ON routes(ttl)")
    .execute(pool)
    .await?;
  sqlx::query(
    "CREATE INDEX IF NOT EXISTS idx_routes_last_verified ON \
     routes(last_verified)",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE INDEX IF NOT EXISTS idx_routes_nar_url ON routes(nar_url)",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    r"CREATE TABLE IF NOT EXISTS upstream_health (
             url TEXT PRIMARY KEY,
             ema_latency REAL NOT NULL DEFAULT 0,
             consecutive_fails INTEGER NOT NULL DEFAULT 0,
             total_queries INTEGER NOT NULL DEFAULT 0
           )",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    r"CREATE TABLE IF NOT EXISTS negative_cache (
             store_path TEXT PRIMARY KEY,
             expires_at INTEGER NOT NULL
           )",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE INDEX IF NOT EXISTS idx_negative_expires ON \
     negative_cache(expires_at)",
  )
  .execute(pool)
  .await?;
  add_column_if_missing(pool, "routes", "narinfo_bytes", "BLOB").await?;
  add_column_if_missing(
    pool,
    "routes",
    "upstream_nar_url",
    "TEXT NOT NULL DEFAULT ''",
  )
  .await?;
  Ok(())
}

fn row_to_route(row: &SqliteRow) -> Result<RouteEntry, DbError> {
  let query_count = row.get::<i64, _>("query_count");
  let failure_count = row.get::<i64, _>("failure_count");
  let nar_size = row.get::<i64, _>("nar_size");
  Ok(RouteEntry {
    store_path:       row.get("store_path"),
    upstream_url:     row.get("upstream_url"),
    latency_ms:       row.get("latency_ms"),
    latency_ema:      row.get("latency_ema"),
    query_count:      u32::try_from(query_count).map_err(|_| {
      DbError::InvalidData(format!("query_count out of range: {query_count}"))
    })?,
    failure_count:    u32::try_from(failure_count).map_err(|_| {
      DbError::InvalidData(format!(
        "failure_count out of range: {failure_count}"
      ))
    })?,
    last_verified:    timestamp(row.get("last_verified"), "last_verified")?,
    ttl:              timestamp(row.get("ttl"), "ttl")?,
    nar_hash:         row.get("nar_hash"),
    nar_size:         u64::try_from(nar_size).map_err(|_| {
      DbError::InvalidData(format!("nar_size out of range: {nar_size}"))
    })?,
    nar_url:          row.get("nar_url"),
    upstream_nar_url: row.get("upstream_nar_url"),
    narinfo_bytes:    row.get("narinfo_bytes"),
  })
}

fn timestamp(
  value: i64,
  field: &'static str,
) -> Result<DateTime<Utc>, DbError> {
  Utc.timestamp_opt(value, 0).single().ok_or_else(|| {
    DbError::InvalidData(format!("{field} timestamp out of range: {value}"))
  })
}

#[cfg(test)]
mod tests {
  use std::{
    env,
    error::Error,
    fs,
    iter,
    process,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
  };

  use tokio::sync::Barrier;

  use super::*;

  const SLOW_STATEMENT_THRESHOLD: Duration = Duration::from_secs(1);

  #[tokio::test]
  async fn route_roundtrip_and_negative_cache() -> Result<(), DbError> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    let entry = RouteEntry {
      store_path:       "abc123".into(),
      upstream_url:     "https://cache.nixos.org".into(),
      latency_ms:       10.0,
      latency_ema:      10.0,
      last_verified:    now,
      query_count:      1,
      failure_count:    0,
      ttl:              now + chrono::Duration::hours(1),
      nar_hash:         "sha256:abc".into(),
      nar_size:         42,
      nar_url:          "nar/abc.nar.xz".into(),
      upstream_nar_url: "nar/abc.nar.xz".into(),
      narinfo_bytes:    None,
    };
    db.set_route(&entry).await?;
    let got = db
      .get_route("abc123")
      .await?
      .ok_or(sqlx::Error::RowNotFound)?;
    assert_eq!(got.upstream_url, entry.upstream_url);
    assert!(db.get_route_by_nar_url("nar/abc.nar.xz").await?.is_some());
    db.set_negative("missing", Duration::from_mins(1)).await?;
    assert!(db.is_negative("missing").await?);
    Ok(())
  }

  #[tokio::test]
  async fn narinfo_bytes_roundtrip() -> Result<(), DbError> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    let bytes = b"StorePath: /nix/store/abc\n".to_vec();
    let entry = RouteEntry {
      store_path:       "abc".into(),
      upstream_url:     "https://cache.nixos.org".into(),
      latency_ms:       1.0,
      latency_ema:      1.0,
      last_verified:    now,
      query_count:      1,
      failure_count:    0,
      ttl:              now + chrono::Duration::hours(1),
      nar_hash:         "sha256:abc".into(),
      nar_size:         26,
      nar_url:          "nar/abc.nar".into(),
      upstream_nar_url: "nar/abc.nar".into(),
      narinfo_bytes:    Some(bytes.clone()),
    };
    db.set_route(&entry).await?;
    let got = db
      .get_route("abc")
      .await?
      .ok_or_else(|| DbError::InvalidData("route not found".to_string()))?;
    assert_eq!(got.narinfo_bytes, Some(bytes));
    Ok(())
  }

  #[tokio::test]
  async fn concurrent_reads_do_not_deadlock() -> Result<(), Box<dyn Error>> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    let entry = RouteEntry {
      store_path:       "aaa".into(),
      upstream_url:     "https://cache.nixos.org".into(),
      latency_ms:       1.0,
      latency_ema:      1.0,
      last_verified:    now,
      query_count:      1,
      failure_count:    0,
      ttl:              now + chrono::Duration::hours(1),
      nar_hash:         "sha256:x".into(),
      nar_size:         1,
      nar_url:          "nar/x.nar".into(),
      upstream_nar_url: "nar/x.nar".into(),
      narinfo_bytes:    None,
    };
    db.set_route(&entry).await?;
    let db = Arc::new(db);
    let handles: Vec<_> = iter::repeat_with(|| {
      let db = Arc::clone(&db);
      tokio::spawn(async move { db.get_route("aaa").await })
    })
    .take(4)
    .collect();
    for h in handles {
      assert!(h.await??.is_some());
    }
    Ok(())
  }

  #[tokio::test]
  async fn concurrent_file_writes_are_serialized() -> Result<(), Box<dyn Error>>
  {
    let path = env::temp_dir()
      .join(format!(
        "ncro-db-concurrent-writes-{}-{}.db",
        process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
      ))
      .to_string_lossy()
      .into_owned();
    let _ = fs::remove_file(&path);
    let db = Arc::new(Db::open(&path, 100, SLOW_STATEMENT_THRESHOLD).await?);
    let barrier = Arc::new(Barrier::new(65));

    let handles: Vec<_> = (0..64_u64)
      .map(|i| {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
          let now = Utc::now();
          let entry = RouteEntry {
            store_path:       format!("hash{i}"),
            upstream_url:     "https://cache.nixos.org".into(),
            latency_ms:       1.0,
            latency_ema:      1.0,
            last_verified:    now,
            query_count:      1,
            failure_count:    0,
            ttl:              now + chrono::Duration::hours(1),
            nar_hash:         format!("sha256:{i}"),
            nar_size:         1,
            nar_url:          format!("nar/{i}.nar"),
            upstream_nar_url: format!("nar/{i}.nar"),
            narinfo_bytes:    None,
          };
          barrier.wait().await;
          db.set_route(&entry).await?;
          db.set_negative(&format!("missing{i}"), Duration::from_mins(1))
            .await?;
          db.save_health(&format!("https://cache{i}.example"), 1.0, 0, 1)
            .await?;
          Ok::<_, DbError>(())
        })
      })
      .collect();

    barrier.wait().await;
    for handle in handles {
      handle.await??;
    }
    assert_eq!(db.route_count().await?, 64);
    drop(db);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(format!("{path}-shm"));
    let _ = fs::remove_file(format!("{path}-wal"));
    Ok(())
  }

  #[test]
  fn expired_route_is_not_valid() {
    let now = Utc::now();
    let entry = RouteEntry {
      store_path:       "abc".into(),
      upstream_url:     "https://cache.nixos.org".into(),
      latency_ms:       1.0,
      latency_ema:      1.0,
      last_verified:    now,
      query_count:      1,
      failure_count:    0,
      ttl:              now - chrono::Duration::seconds(1),
      nar_hash:         "sha256:abc".into(),
      nar_size:         1,
      nar_url:          "nar/abc.nar".into(),
      upstream_nar_url: "nar/abc.nar".into(),
      narinfo_bytes:    None,
    };
    assert!(!entry.is_valid());
    let fresh = RouteEntry {
      ttl: now + chrono::Duration::hours(1),
      ..entry
    };
    assert!(fresh.is_valid());
  }

  #[tokio::test]
  async fn get_route_by_nar_url_rejects_expired_ttl() -> Result<(), DbError> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    let entry = RouteEntry {
      store_path:       "exp".into(),
      upstream_url:     "https://cache.nixos.org".into(),
      latency_ms:       1.0,
      latency_ema:      1.0,
      last_verified:    now,
      query_count:      1,
      failure_count:    0,
      ttl:              now - chrono::Duration::seconds(1),
      nar_hash:         "sha256:exp".into(),
      nar_size:         1,
      nar_url:          "nar/exp.nar".into(),
      upstream_nar_url: "nar/exp.nar".into(),
      narinfo_bytes:    None,
    };
    db.set_route(&entry).await?;
    assert!(
      db.get_route_by_nar_url("nar/exp.nar").await?.is_none(),
      "expired route must not be returned by nar_url lookup"
    );
    Ok(())
  }

  #[tokio::test]
  async fn expire_old_routes_removes_stale_entries() -> Result<(), DbError> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    let expired = RouteEntry {
      store_path:       "stale".into(),
      upstream_url:     "https://cache.nixos.org".into(),
      latency_ms:       1.0,
      latency_ema:      1.0,
      last_verified:    now,
      query_count:      1,
      failure_count:    0,
      ttl:              now - chrono::Duration::seconds(1),
      nar_hash:         "sha256:stale".into(),
      nar_size:         1,
      nar_url:          "nar/stale.nar".into(),
      upstream_nar_url: "nar/stale.nar".into(),
      narinfo_bytes:    None,
    };
    let fresh = RouteEntry {
      store_path: "fresh".into(),
      nar_hash: "sha256:fresh".into(),
      nar_url: "nar/fresh.nar".into(),
      upstream_nar_url: "nar/fresh.nar".into(),
      ttl: now + chrono::Duration::hours(1),
      ..expired.clone()
    };
    db.set_route(&expired).await?;
    db.set_route(&fresh).await?;
    assert_eq!(db.route_count().await?, 2);
    db.expire_old_routes().await?;
    assert_eq!(db.route_count().await?, 1);
    assert!(db.get_route("fresh").await?.is_some());
    assert!(db.get_route("stale").await?.is_none());
    Ok(())
  }

  #[tokio::test]
  async fn eviction_bounds_table_size() -> Result<(), DbError> {
    let max: i64 = 3;
    let db = Db::open(":memory:", max, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    // 100 writes triggers eviction at write #100 (count 99 mod 100 == 99)
    for i in 0..100u64 {
      let entry = RouteEntry {
        store_path:       format!("hash{i}"),
        upstream_url:     "https://cache.nixos.org".into(),
        latency_ms:       1.0,
        latency_ema:      1.0,
        last_verified:    now,
        query_count:      1,
        failure_count:    0,
        ttl:              now + chrono::Duration::hours(1),
        nar_hash:         format!("sha256:{i}"),
        nar_size:         1,
        nar_url:          format!("nar/{i}.nar"),
        upstream_nar_url: format!("nar/{i}.nar"),
        narinfo_bytes:    None,
      };
      db.set_route(&entry).await?;
    }
    assert!(
      db.route_count().await? <= max,
      "table must be bounded to max_entries={max} after eviction"
    );
    Ok(())
  }

  #[tokio::test]
  async fn eviction_is_throttled() -> Result<(), DbError> {
    let db = Db::open(":memory:", 2, SLOW_STATEMENT_THRESHOLD).await?;
    let now = Utc::now();
    for i in 0..3u64 {
      let entry = RouteEntry {
        store_path:       format!("hash{i}"),
        upstream_url:     "https://cache.nixos.org".into(),
        latency_ms:       1.0,
        latency_ema:      1.0,
        last_verified:    now,
        query_count:      1,
        failure_count:    0,
        ttl:              now + chrono::Duration::hours(1),
        nar_hash:         format!("sha256:{i}"),
        nar_size:         1,
        nar_url:          format!("nar/{i}.nar"),
        upstream_nar_url: format!("nar/{i}.nar"),
        narinfo_bytes:    None,
      };
      db.set_route(&entry).await?;
    }
    assert!(db.get_route("hash0").await.is_ok());
    Ok(())
  }
}
