use std::{
  io::{self, BufRead, BufReader, Read},
  num::ParseIntError,
};

use base64::{
  DecodeError,
  Engine,
  alphabet,
  engine::{
    DecodePaddingMode,
    GeneralPurpose,
    GeneralPurposeConfig,
    general_purpose::STANDARD,
  },
};

/// Base64 engine that accepts the standard alphabet with or without trailing
/// `=` padding. Nix's `trusted-public-keys` format and the keys emitted by
/// tools like `atticd` are not consistent about padding so decoding must
/// tolerate both.
const STANDARD_LENIENT: GeneralPurpose = GeneralPurpose::new(
  &alphabet::STANDARD,
  GeneralPurposeConfig::new()
    .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NarInfoError {
  #[error("read narinfo: {0}")]
  Io(#[from] io::Error),
  #[error("malformed line: {0:?}")]
  MalformedLine(String),
  #[error("missing StorePath")]
  MissingStorePath,
  #[error("{field}: {source}")]
  ParseInt {
    field:  &'static str,
    source: ParseIntError,
  },
  #[error("invalid public key {input:?}: missing ':'")]
  MissingPublicKeySeparator { input: String },
  #[error("invalid public key {input:?}: {source}")]
  InvalidPublicKeyBase64 { input: String, source: DecodeError },
  #[error("invalid public key size {got}, want 32")]
  InvalidPublicKeySize { got: usize },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NarInfo {
  pub store_path:  String,
  pub url:         String,
  pub compression: String,
  pub file_hash:   String,
  pub file_size:   u64,
  pub nar_hash:    String,
  pub nar_size:    u64,
  pub references:  Vec<String>,
  pub deriver:     String,
  pub sig:         Vec<String>,
  pub ca:          String,
}

/// Nix's custom base32 alphabet (not RFC 4648 base32).
const NIX_BASE32_ALPHABET: &[u8; 32] = b"0123456789abcdfghijklmnpqrsvwxyz";

/// Encodes `bytes` using Nix's custom base32. For a 32-byte sha256 digest
/// this produces a 52-character string. This seems to be the canonical
/// representation used in `narinfo` `NarHash` fields and in Nix-style signature
/// fingerprints.
#[must_use]
fn nix_base32_encode(bytes: &[u8]) -> String {
  if bytes.is_empty() {
    return String::new();
  }
  let len = (bytes.len() * 8 - 1) / 5 + 1;
  let mut out = String::with_capacity(len);
  for n in (0..len).rev() {
    let b = n * 5;
    let i = b / 8;
    let j = b % 8;
    let c = (u32::from(bytes[i]) >> j)
      | bytes.get(i + 1).map_or(0, |&b| u32::from(b) << (8 - j));
    out.push(char::from(NIX_BASE32_ALPHABET[(c & 0x1F) as usize]));
  }
  out
}

/// Normalizes a Nix narinfo hash field to its canonical `<algo>:<base32>`
/// representation. Some upstreams (notably `atticd`) serialize `NarHash` as
/// hex (`sha256:<64 hex chars>`) even though Nix signs the fingerprint over
/// the base32 form, so verifying signatures against attic-served narinfos
/// requires converting hex to base32 before recomputing the fingerprint.
/// Inputs that are not recognizable as `sha256:` hex are returned unchanged.
#[must_use]
fn normalize_nar_hash(s: &str) -> String {
  let Some((algo, digest)) = s.split_once(':') else {
    return s.to_string();
  };
  // sha256: 32 bytes -> 64 hex chars or 52 nix-base32 chars. The two
  // alphabets overlap on every hex character, so length is the unambiguous
  // discriminator. Only convert when the input is unambiguously hex.
  if algo == "sha256"
    && digest.len() == 64
    && digest.bytes().all(|b| b.is_ascii_hexdigit())
    && let Ok(bytes) = hex::decode(digest)
  {
    return format!("{algo}:{}", nix_base32_encode(&bytes));
  }
  s.to_string()
}

/// # Errors
///
/// Returns [`NarInfoError`] if the input lacks a `name:base64` separator,
/// the name is empty, the base64 is invalid, or the key is not 32 bytes.
pub fn parse_public_key(
  input: &str,
) -> Result<(String, VerifyingKey), NarInfoError> {
  let (name, b64) = input.split_once(':').ok_or_else(|| {
    NarInfoError::MissingPublicKeySeparator {
      input: input.to_string(),
    }
  })?;
  if name.is_empty() {
    return Err(NarInfoError::MissingPublicKeySeparator {
      input: input.to_string(),
    });
  }
  let raw = STANDARD_LENIENT.decode(b64).map_err(|source| {
    NarInfoError::InvalidPublicKeyBase64 {
      input: input.to_string(),
      source,
    }
  })?;
  let bytes: [u8; 32] = raw.try_into().map_err(|raw: Vec<u8>| {
    NarInfoError::InvalidPublicKeySize { got: raw.len() }
  })?;
  let key = VerifyingKey::from_bytes(&bytes)
    .map_err(|_| NarInfoError::InvalidPublicKeySize { got: bytes.len() })?;
  Ok((name.to_string(), key))
}

impl NarInfo {
  /// # Errors
  ///
  /// Returns [`NarInfoError`] if a line is malformed, an integer field cannot
  /// be parsed, an I/O error occurs, or `StorePath` is missing.
  pub fn parse(reader: impl Read) -> Result<Self, NarInfoError> {
    let mut narinfo = Self::default();
    for line in BufReader::new(reader).lines() {
      let line = line?;
      if line.is_empty() {
        continue;
      }
      let (key, value) = line
        .split_once(": ")
        .ok_or_else(|| NarInfoError::MalformedLine(line.clone()))?;
      match key {
        "StorePath" => narinfo.store_path = value.to_string(),
        "URL" => narinfo.url = value.to_string(),
        "Compression" => narinfo.compression = value.to_string(),
        "FileHash" => narinfo.file_hash = value.to_string(),
        "FileSize" => {
          narinfo.file_size = value.parse().map_err(|source| {
            NarInfoError::ParseInt {
              field: "FileSize",
              source,
            }
          })?;
        },
        "NarHash" => narinfo.nar_hash = value.to_string(),
        "NarSize" => {
          narinfo.nar_size = value.parse().map_err(|source| {
            NarInfoError::ParseInt {
              field: "NarSize",
              source,
            }
          })?;
        },
        "References" if !value.is_empty() => {
          narinfo.references =
            value.split_whitespace().map(str::to_string).collect();
        },
        "Deriver" => narinfo.deriver = value.to_string(),
        "Sig" => narinfo.sig.push(value.to_string()),
        "CA" => narinfo.ca = value.to_string(),
        _ => {},
      }
    }
    if narinfo.store_path.is_empty() {
      return Err(NarInfoError::MissingStorePath);
    }
    Ok(narinfo)
  }

  #[must_use]
  pub fn fingerprint(&self) -> String {
    let refs = self
      .references
      .iter()
      .map(|reference| {
        if reference.starts_with("/nix/store/") {
          reference.clone()
        } else {
          format!("/nix/store/{reference}")
        }
      })
      .collect::<Vec<_>>()
      .join(",");
    format!(
      "1;{};{};{};{}",
      self.store_path,
      normalize_nar_hash(&self.nar_hash),
      self.nar_size,
      refs
    )
  }

  /// # Errors
  ///
  /// Returns [`NarInfoError`] if `public_key` cannot be parsed (see
  /// [`parse_public_key`]).
  pub fn verify(&self, public_key: &str) -> Result<bool, NarInfoError> {
    let (key_name, key) = parse_public_key(public_key)?;
    let fingerprint = self.fingerprint();
    for sig_line in &self.sig {
      let Some((name, b64)) = sig_line.split_once(':') else {
        continue;
      };
      if name != key_name {
        continue;
      }
      let Ok(raw) = STANDARD.decode(b64) else {
        continue;
      };
      let Ok(bytes) = <[u8; 64]>::try_from(raw.as_slice()) else {
        continue;
      };
      let signature = Signature::from_bytes(&bytes);
      if key.verify(fingerprint.as_bytes(), &signature).is_ok() {
        return Ok(true);
      }
    }
    Ok(false)
  }
}

#[cfg(test)]
mod tests {
  use ed25519_dalek::{Signer, SigningKey};
  use rand::RngExt;

  use super::*;

  #[test]
  fn parses_realistic_narinfo() -> Result<(), NarInfoError> {
    let input = "StorePath: /nix/store/abc-hello\nURL: \
                 nar/abc.nar.xz\nCompression: xz\nFileSize: 42\nNarHash: \
                 sha256:abc\nNarSize: 123\nReferences: abc-hello dep\nSig: \
                 key:sig=\n";
    let ni = NarInfo::parse(input.as_bytes())?;
    assert_eq!(ni.store_path, "/nix/store/abc-hello");
    assert_eq!(ni.url, "nar/abc.nar.xz");
    assert_eq!(ni.references.len(), 2);
    Ok(())
  }

  #[test]
  fn missing_store_path_returns_error() {
    let input = "URL: nar/abc.nar.xz\nNarHash: sha256:abc\nNarSize: 1\n";
    assert!(matches!(
      NarInfo::parse(input.as_bytes()),
      Err(NarInfoError::MissingStorePath)
    ));
  }

  #[test]
  fn malformed_line_returns_error() {
    let input = "StorePath: /nix/store/abc\nno-colon-here\n";
    assert!(matches!(
      NarInfo::parse(input.as_bytes()),
      Err(NarInfoError::MalformedLine(_))
    ));
  }

  #[test]
  fn parse_public_key_error_paths() {
    assert!(matches!(
      parse_public_key("no-separator"),
      Err(NarInfoError::MissingPublicKeySeparator { .. })
    ));
    // Empty name before colon
    assert!(matches!(
      parse_public_key(":dGVzdA=="),
      Err(NarInfoError::MissingPublicKeySeparator { .. })
    ));
    // Valid name but invalid base64
    assert!(matches!(
      parse_public_key("test:!!!"),
      Err(NarInfoError::InvalidPublicKeyBase64 { .. })
    ));
    // Valid base64 but wrong length (not 32 bytes)
    assert!(matches!(
      parse_public_key("test:dGVzdA=="),
      Err(NarInfoError::InvalidPublicKeySize { .. })
    ));
  }

  #[test]
  fn nix_base32_encodes_known_vectors() {
    // Empty input -> empty output.
    assert_eq!(nix_base32_encode(&[]), "");
    // sha256 of the empty string, cross-checked against
    //   `nix hash to-base32 --type sha256 e3b0...b855`.
    let empty_sha256: [u8; 32] = [
      0xE3, 0xB0, 0xC4, 0x42, 0x98, 0xFC, 0x1C, 0x14, 0x9A, 0xFB, 0xF4, 0xC8,
      0x99, 0x6F, 0xB9, 0x24, 0x27, 0xAE, 0x41, 0xE4, 0x64, 0x9B, 0x93, 0x4C,
      0xA4, 0x95, 0x99, 0x1B, 0x78, 0x52, 0xB8, 0x55,
    ];
    let encoded = nix_base32_encode(&empty_sha256);
    assert_eq!(
      encoded,
      "0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73"
    );
    assert_eq!(encoded.len(), 52);
  }

  #[test]
  fn normalize_nar_hash_converts_hex_to_base32() {
    let hex_form =
      "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let base32_form =
      "sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73";
    // hex form is normalized
    assert_eq!(normalize_nar_hash(hex_form), base32_form);
    // base32 form passes through unchanged
    assert_eq!(normalize_nar_hash(base32_form), base32_form);
    // unknown algorithms pass through
    assert_eq!(normalize_nar_hash("md5:abc"), "md5:abc");
    // missing separator passes through
    assert_eq!(normalize_nar_hash("notahash"), "notahash");
  }

  #[test]
  fn fingerprint_normalizes_attic_style_hex_nar_hash() {
    // Regression: attic serves NarHash as hex but signs over base32, so two
    // NarInfos that differ only in NarHash encoding must produce identical
    // fingerprints.
    let common = NarInfo {
      store_path: "/nix/store/abc-test".into(),
      nar_size: 312,
      references: vec![],
      ..Default::default()
    };
    let mut hex_ni = common.clone();
    hex_ni.nar_hash =
      "sha256:08a270a0c126f730839d0ef8bc86af8dd95c6d2bd09510b427adfb9540539e87"
        .into();
    let mut b32_ni = common;
    let bytes: [u8; 32] = [
      0x08, 0xA2, 0x70, 0xA0, 0xC1, 0x26, 0xF7, 0x30, 0x83, 0x9D, 0x0E, 0xF8,
      0xBC, 0x86, 0xAF, 0x8D, 0xD9, 0x5C, 0x6D, 0x2B, 0xD0, 0x95, 0x10, 0xB4,
      0x27, 0xAD, 0xFB, 0x95, 0x40, 0x53, 0x9E, 0x87,
    ];
    b32_ni.nar_hash = format!("sha256:{}", nix_base32_encode(&bytes));

    assert_eq!(hex_ni.fingerprint(), b32_ni.fingerprint());
  }

  #[test]
  fn parse_public_key_tolerates_missing_padding() -> Result<(), NarInfoError> {
    // Regression for issue #19. The previous implementation used the
    // padding-strict `STANDARD` base64 engine and rejected 43-character
    // unpadded keys with `Invalid padding`, even though Nix accepts them.
    let mut seed = [0_u8; 32];
    rand::rng().fill(&mut seed);
    let signing = SigningKey::from_bytes(&seed);
    let bytes = signing.verifying_key().to_bytes();
    let unpadded = STANDARD.encode(bytes).trim_end_matches('=').to_string();
    assert_eq!(unpadded.len(), 43);
    assert!(!unpadded.ends_with('='));

    let (name, parsed) = parse_public_key(&format!("cache:{unpadded}"))?;
    assert_eq!(name, "cache");
    assert_eq!(parsed.to_bytes(), bytes);
    Ok(())
  }

  #[test]
  fn parse_public_key_accepts_padded_and_unpadded() -> Result<(), NarInfoError>
  {
    // Generate a real keypair so the verifying key is guaranteed to be a
    // valid ed25519 curve point (ed25519-dalek's `from_bytes` rejects
    // arbitrary 32-byte inputs that don't decode to a point on the curve).
    let mut seed = [0_u8; 32];
    rand::rng().fill(&mut seed);
    let signing = SigningKey::from_bytes(&seed);
    let bytes = signing.verifying_key().to_bytes();
    let padded = STANDARD.encode(bytes);
    let unpadded = padded.trim_end_matches('=').to_string();
    assert_eq!(padded.len(), 44);
    assert_eq!(unpadded.len(), 43);

    let (_, k_padded) = parse_public_key(&format!("name:{padded}"))?;
    let (_, k_unpadded) = parse_public_key(&format!("name:{unpadded}"))?;
    assert_eq!(k_padded.to_bytes(), k_unpadded.to_bytes());
    Ok(())
  }

  #[test]
  fn verifies_roundtrip_signature() -> Result<(), NarInfoError> {
    let mut key_bytes = [0_u8; 32];
    rand::rng().fill(&mut key_bytes);
    let signing = SigningKey::from_bytes(&key_bytes);
    let mut ni = NarInfo {
      store_path: "/nix/store/abc-test".into(),
      nar_hash: "sha256:abc".into(),
      nar_size: 12,
      references: vec!["abc-test".into()],
      ..Default::default()
    };
    let sig = signing.sign(ni.fingerprint().as_bytes());
    let pubkey = format!(
      "test:{}",
      STANDARD.encode(signing.verifying_key().to_bytes())
    );
    ni.sig = vec![format!("test:{}", STANDARD.encode(sig.to_bytes()))];
    assert!(ni.verify(&pubkey)?);
    Ok(())
  }

  #[test]
  fn verify_with_wrong_key_returns_false() -> Result<(), NarInfoError> {
    let mut key1_bytes = [0u8; 32];
    let mut key2_bytes = [1u8; 32];
    rand::rng().fill(&mut key1_bytes);
    rand::rng().fill(&mut key2_bytes);
    let signing1 = SigningKey::from_bytes(&key1_bytes);
    let signing2 = SigningKey::from_bytes(&key2_bytes);
    let mut ni = NarInfo {
      store_path: "/nix/store/abc-test".into(),
      nar_hash: "sha256:abc".into(),
      nar_size: 12,
      ..Default::default()
    };
    let sig = signing1.sign(ni.fingerprint().as_bytes());
    ni.sig = vec![format!("test:{}", STANDARD.encode(sig.to_bytes()))];
    let wrong_pubkey = format!(
      "test:{}",
      STANDARD.encode(signing2.verifying_key().to_bytes())
    );
    assert!(!ni.verify(&wrong_pubkey)?);
    Ok(())
  }

  #[test]
  fn verify_tampered_content_returns_false() -> Result<(), NarInfoError> {
    let mut key_bytes = [0u8; 32];
    rand::rng().fill(&mut key_bytes);
    let signing = SigningKey::from_bytes(&key_bytes);
    let mut ni = NarInfo {
      store_path: "/nix/store/abc-test".into(),
      nar_hash: "sha256:abc".into(),
      nar_size: 12,
      ..Default::default()
    };
    let sig = signing.sign(ni.fingerprint().as_bytes());
    let pubkey = format!(
      "test:{}",
      STANDARD.encode(signing.verifying_key().to_bytes())
    );
    ni.sig = vec![format!("test:{}", STANDARD.encode(sig.to_bytes()))];
    ni.nar_size = 999; // tamper after signing
    assert!(!ni.verify(&pubkey)?);
    Ok(())
  }
}
