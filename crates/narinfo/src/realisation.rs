//! Content-addressed realisations, the `.doi` files that map a derivation
//! output to its store path.

use flub::Error;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{NarInfoError, verify_fingerprint};

#[derive(Debug, Error)]
#[error("parse realisation JSON")]
pub struct RealisationError {
  source: serde_json::Error,
}

/// The derivation output a `.doi` was fetched for.
#[derive(Clone, Copy)]
pub enum DrvOutput<'a> {
  /// `build-trace-v2/<drv>/<output>.doi`, written by Nix 2.35 and later.
  BuildTrace {
    drv_path:    &'a str,
    output_name: &'a str,
  },
  /// `realisations/<drv hash>!<output>.doi`, written by Nix 2.34 and earlier.
  Legacy,
}

/// Nix accepts a signature either as `name:base64` or as an object.
#[derive(Deserialize)]
#[serde(untagged)]
enum WireSignature {
  Text(String),
  Object {
    #[serde(rename = "keyName")]
    key_name: String,
    sig:      String,
  },
}

impl From<WireSignature> for String {
  fn from(signature: WireSignature) -> Self {
    match signature {
      WireSignature::Text(text) => text,
      WireSignature::Object { key_name, sig } => format!("{key_name}:{sig}"),
    }
  }
}

#[derive(Deserialize)]
struct BuildTraceBody {
  #[serde(rename = "outPath")]
  out_path:   String,
  #[serde(default)]
  signatures: Vec<WireSignature>,
}

/// Nix's JSON sorts keys, so fields stay alphabetical.
#[derive(Serialize)]
struct BuildTraceFingerprint<'a> {
  key:   BuildTraceKey<'a>,
  value: BuildTraceValue<'a>,
}

#[derive(Serialize)]
struct BuildTraceKey<'a> {
  #[serde(rename = "drvPath")]
  drv_path:    &'a str,
  #[serde(rename = "outputName")]
  output_name: &'a str,
}

#[derive(Serialize)]
struct BuildTraceValue<'a> {
  #[serde(rename = "outPath")]
  out_path: &'a str,
}

pub struct Realisation {
  fingerprint: String,
  signatures:  Vec<String>,
}

impl Realisation {
  /// Parses a `.doi` body and computes the fingerprint its signatures cover.
  ///
  /// # Errors
  ///
  /// Returns [`RealisationError`] if the body is not the JSON Nix writes.
  pub fn parse(
    body: &[u8],
    id: DrvOutput<'_>,
  ) -> Result<Self, RealisationError> {
    match id {
      DrvOutput::BuildTrace {
        drv_path,
        output_name,
      } => {
        let body = serde_json::from_slice::<BuildTraceBody>(body)
          .map_err(|source| RealisationError { source })?;
        let fingerprint = serde_json::to_string(&BuildTraceFingerprint {
          key:   BuildTraceKey {
            drv_path,
            output_name,
          },
          value: BuildTraceValue {
            out_path: &body.out_path,
          },
        })
        .map_err(|source| RealisationError { source })?;

        Ok(Self {
          fingerprint,
          signatures: body.signatures.into_iter().map(String::from).collect(),
        })
      },

      // Legacy Nix signs its own JSON minus `signatures`.
      DrvOutput::Legacy => {
        let mut fields = serde_json::from_slice::<Map<String, Value>>(body)
          .map_err(|source| RealisationError { source })?;
        let signatures = fields
          .remove("signatures")
          .map(serde_json::from_value::<Vec<WireSignature>>)
          .transpose()
          .map_err(|source| RealisationError { source })?
          .unwrap_or_default();

        Ok(Self {
          fingerprint: Value::Object(fields).to_string(),
          signatures:  signatures.into_iter().map(String::from).collect(),
        })
      },
    }
  }

  /// # Errors
  ///
  /// Returns [`NarInfoError`] if `public_key` cannot be parsed.
  pub fn verify(&self, public_key: &str) -> Result<bool, NarInfoError> {
    verify_fingerprint(&self.fingerprint, &self.signatures, public_key)
  }
}
