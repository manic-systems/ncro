use std::{
  env,
  error::Error,
  fs,
  process::{self, Command},
};

fn ncro() -> Command {
  Command::new(env!("CARGO_BIN_EXE_ncro"))
}

#[test]
fn generate_mesh_key_persists_identity_and_validates_arguments()
-> Result<(), Box<dyn Error>> {
  let dir =
    env::temp_dir().join(format!("ncro-mesh-key-cli-{}", process::id()));
  fs::create_dir_all(&dir)?;
  let key_path = dir.join("node.key");

  let first = ncro().arg("--generate-mesh-key").arg(&key_path).output()?;
  let second = ncro().arg("--generate-mesh-key").arg(&key_path).output()?;

  assert!(first.status.success());
  assert_eq!(first.stdout, second.stdout);
  assert_eq!(first.stdout.strip_suffix(b"\n").map(<[u8]>::len), Some(64));
  assert!(first.stderr.is_empty());
  assert_eq!(fs::metadata(&key_path)?.len(), 32);

  let empty_path = ncro().arg("--generate-mesh-key=").output()?;
  assert!(!empty_path.status.success());

  let conflicting_options = ncro()
    .arg("--config")
    .arg("unused.toml")
    .arg("--generate-mesh-key")
    .arg(&key_path)
    .output()?;
  assert!(!conflicting_options.status.success());

  fs::remove_dir_all(dir)?;
  Ok(())
}
