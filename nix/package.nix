{
  lib,
  stdenv,
  rustPlatform,
  installShellFiles,
  pkg-config,
  openssl,
  cacert,
  buildPackages,
  wild ? null,
}: let
  clangExe = "${buildPackages.clang}/bin/${stdenv.cc.targetPrefix}clang";
  # wild + clang are only used on Linux tier-1 arches
  hasWild =
    stdenv.hostPlatform.isLinux && (stdenv.hostPlatform.isx86_64 || stdenv.hostPlatform.isAarch64);
  cargoTOML = (lib.importTOML ../Cargo.toml).workspace.package;
in
  rustPlatform.buildRustPackage (finalAttrs: {
    pname = "ncro";
    inherit (cargoTOML) version;

    src = let
      fs = lib.fileset;
      s = ../.;
    in
      fs.toSource {
        root = s;
        fileset = fs.unions [
          (s + /ncro)
          (s + /crates)
          (s + /Cargo.toml)
          (s + /Cargo.lock)
        ];
      };

    useNextest = true;
    cargoLock.lockFile = "${finalAttrs.src}/Cargo.lock";
    nativeBuildInputs =
      [pkg-config cacert installShellFiles]
      ++ (lib.optionals hasWild [
        wild
        buildPackages.clang
      ]);

    buildInputs = [
      openssl.dev
    ];

    env =
      {
        # reqwest (rustls) needs a CA bundle to construct a TLS client, even in
        # tests that never make network requests.
        SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";

        # Link nixpkgs c libs, no vendored copies.
        OPENSSL_NO_VENDOR = 1;
      }
      // lib.optionalAttrs hasWild {
        RUSTFLAGS = "-Clinker=${clangExe} -Clink-arg=--ld-path=wild";
      };

    postInstall = lib.optionalString (stdenv.buildPlatform.canExecute stdenv.hostPlatform) ''
      installShellCompletion --cmd ncro \
        --bash    <(COMPLETE=bash $out/bin/ncro) \
        --zsh     <(COMPLETE=zsh  $out/bin/ncro)  \
        --fish    <(COMPLETE=fish $out/bin/ncro) \
        --nushell <(COMPLETE=nu   $out/bin/ncro)
    '';

    meta = {
      homepage = "https://github.com/feel-co/ncro";
      license = lib.licenses.eupl12;
      mainProgram = "ncro";
      maintainers = with lib.maintainers; [NotAShelf];
    };
  })
