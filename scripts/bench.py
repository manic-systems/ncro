#!/usr/bin/env python3
"""
ncro narinfo latency benchmark.

Usage:
  python3 bench.py [--ncro-bin PATH] [--out FILE] [--direct-rounds N] [--verbose]
"""

import argparse
import os
import signal
import socket
import sqlite3 as sq3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

NCRO_PORT = 17823
NCRO_HOST = f"http://127.0.0.1:{NCRO_PORT}"

# Packages I consider stable and "always present" from cache.nixos.org
# On eval failure we skip silently. Though, someone would notice if
# those were ever broken and it'd be fixed before I could even get
# the error.
PACKAGES = [
    "jq",
    "ripgrep",
    "curl",
    "fd",
    "bat",
    "zstd",
    "xz",
    "gzip",
    "bzip2",
    "htop",
    "vim",
    "git",
    "tmux",
    "fzf",
    "direnv",
    "wget",
    "tree",
    "which",
    "less",
    "bash",
    "sqlite",
    "openssl",
    "file",
    "diffutils",
    "gnused",
    "gnugrep",
    "findutils",
    "coreutils",
    "gawk",
]

UPSTREAM_URL = "https://cache.nixos.org"
UPSTREAM_PUBKEY = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
TRIM = 0.10


def find_ncro(hint: str | None) -> Path:
    if hint:
        p = Path(hint)
        if p.exists():
            return p
        sys.exit(f"ncro binary not found at {hint}")
    for candidate in [Path("target/release/ncro"), Path("target/debug/ncro")]:
        if candidate.exists():
            return candidate
    sys.exit(
        "ncro binary not found.\n"
        "Build with:  nix develop --command cargo build --release"
    )


def get_store_hashes(packages: list[str]) -> list[str]:
    hashes: list[str] = []
    for pkg in packages:
        try:
            r = subprocess.run(
                ["nix", "eval", "--raw", f"nixpkgs#{pkg}.outPath"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode == 0:
                store_path = r.stdout.strip()
                h = store_path.removeprefix("/nix/store/").split("-")[0]
                hashes.append(h)
                print(f"  {pkg}: {h}", file=sys.stderr)
        except Exception as exc:
            print(f"  {pkg}: skip ({exc})", file=sys.stderr)
    if not hashes:
        sys.exit("No store hashes resolved. Is nix in PATH?")
    return hashes


def write_ncro_config(path: str, db_path: str) -> None:
    with open(path, "w") as f:
        f.write(f"""\
[server]
listen        = ":{NCRO_PORT}"
read_timeout  = "30s"
write_timeout = "30s"

[cache]
db_path       = "{db_path}"
latency_alpha = 0.3
max_entries   = 100000
negative_ttl  = "10m"
ttl           = "1h"

[cache.mass_query]
in_memory_negative_ttl    = "5s"
max_concurrent_races      = 64
per_upstream_max_inflight = 8
upstream_cooldown         = "15s"

[discovery]
enabled = false

[mesh]
enabled = false

[logging]
format = "text"
level  = "warn"

[[upstreams]]
priority   = 10
public_key = "{UPSTREAM_PUBKEY}"
url        = "{UPSTREAM_URL}"
""")


def fetch_ms(url: str) -> float | None:
    """GET url via curl with a fresh TCP connection per call."""
    try:
        r = subprocess.run(
            [
                "curl",
                "-s",
                "--no-keepalive",
                "--http1.1",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code} %{time_total}",
                "--max-time",
                "20",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=25,
        )
        parts = r.stdout.strip().split()
        if len(parts) != 2:
            return None
        code, secs = int(parts[0]), float(parts[1])
        if code == 0 or code == 404:
            return None
        return secs * 1000
    except Exception:
        return None


def measure_narinfo(
    hashes: list[str],
    base_url: str,
    rounds: int,
    verbose: bool = False,
) -> list[float]:
    """Return raw latency samples (ms) for all hashes across all rounds."""
    samples: list[float] = []
    for h in hashes:
        url = f"{base_url}/{h}.narinfo"
        for i in range(rounds):
            v = fetch_ms(url)
            if v is not None:
                samples.append(v)
                if verbose:
                    print(f"    [{h[:8]}] r{i+1}: {v:.1f}ms", file=sys.stderr)
            elif verbose:
                print(f"    [{h[:8]}] r{i+1}: skip", file=sys.stderr)
    return samples


def trimmed_stats(
    samples: list[float], trim: float = TRIM
) -> tuple[float, float, float, float, int]:
    """Compute stats after discarding `trim` fraction from each tail."""
    if not samples:
        return 0.0, 0.0, 0.0, 0.0, 0
    s = sorted(samples)
    n = len(s)
    cut = int(n * trim)
    trimmed = s[cut : n - cut] if n > 2 * cut else s[:]
    mean = statistics.mean(trimmed)
    stdev = statistics.stdev(trimmed) if len(trimmed) > 1 else 0.0
    p50 = s[n // 2]
    p95 = s[min(n - 1, int(n * 0.95))]
    return mean, stdev, p50, p95, len(trimmed)


def count_sqlite_routes(db_path: str) -> int | None:
    """Count rows in the routes table of the ncro SQLite DB."""
    try:
        conn = sq3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
        conn.close()
        return count
    except Exception:
        return None


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def wait_for_ncro(timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            ["curl", "-sf", "--max-time", "1", f"{NCRO_HOST}/nix-cache-info"],
            capture_output=True,
        )
        if r.returncode == 0:
            return True
        time.sleep(0.25)
    return False


def stop_ncro(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def start_ncro(ncro_bin: Path, config_path: str) -> subprocess.Popen:
    return subprocess.Popen(
        [str(ncro_bin), "--config", config_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def speedup_text(val: float, baseline: float) -> str:
    if baseline <= 0:
        return ""
    ratio = baseline / val
    if abs(ratio - 1.0) < 0.05:
        return "approx. baseline"
    if ratio > 1.0:
        return f"{ratio:.0f}x faster"
    return f"{1/ratio:.1f}x slower"


def render_chart(
    bars: list[
        tuple[str, str, float, float, str]
    ],  # (label, sublabel, mean_ms, stdev_ms, fill)
    title: str,
    subtitle: str,
    out_path: Path,
) -> None:
    """Render a grayscale bar chart to SVG with matplotlib. """
    try:
        import matplotlib
    except ImportError:
        sys.exit("matplotlib is required to render the chart.")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "serif",
            # Keep text as text (not outlined paths) so the SVG stays small
            # and editable.
            "svg.fonttype": "none",
        }
    )

    labels = [b[0] for b in bars]
    sublabels = [b[1] for b in bars]
    means = [b[2] for b in bars]
    stdevs = [b[3] for b in bars]
    fills = [b[4] for b in bars]
    positions = range(len(bars))

    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    ax.bar(
        positions,
        means,
        width=0.5,
        color=fills,
        edgecolor="black",
        linewidth=0.5,
        yerr=stdevs,
        error_kw={"ecolor": "black", "elinewidth": 1.0, "capsize": 4},
    )

    for pos, mean in zip(positions, means):
        ax.text(pos, mean, f"{mean:.0f} ms", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(list(positions))
    ax.set_xticklabels([f"{label}\n{sub}" for label, sub in zip(labels, sublabels)])
    ax.set_ylabel("latency (ms)")
    ax.set_ylim(0, (max(means) * 1.18) if means else 1.0)
    ax.yaxis.grid(True, linestyle="--", color="#cccccc", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.suptitle(title, fontsize=13, fontweight="bold", y=0.98)
    ax.set_title(subtitle, fontsize=9, fontstyle="italic", color="#555555")

    fig.tight_layout()
    fig.savefig(out_path, format="svg")
    plt.close(fig)


def print_stats(label: str, samples: list[float]) -> None:
    mean, stdev, p50, p95, count = trimmed_stats(samples)
    total = len(samples)
    print(
        f"  {label:<20} mean={mean:.1f}ms  s={stdev:.1f}  "
        f"p50={p50:.1f}  p95={p95:.1f}  n={count}/{total}",
        file=sys.stderr,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ncro-bin", help="Path to ncro binary")
    ap.add_argument(
        "--out", default="benchmark.svg", help="Output SVG (default: benchmark.svg)"
    )
    ap.add_argument(
        "--direct-rounds",
        type=int,
        default=3,
        help="Requests per hash for the direct baseline (default 3). "
        "ncro cold and warm always use 1 round per hash.",
    )
    ap.add_argument("--verbose", action="store_true", help="Print per-request timings")
    args = ap.parse_args()

    if port_in_use(NCRO_PORT):
        sys.exit(f"Port {NCRO_PORT} is already in use.")

    ncro_bin = find_ncro(args.ncro_bin)
    print(f"Using ncro: {ncro_bin}", file=sys.stderr)

    print("Resolving store hashes from nixpkgs...", file=sys.stderr)
    hashes = get_store_hashes(PACKAGES)
    n_hashes = len(hashes)
    print(f"Got {n_hashes} hashes.\n", file=sys.stderr)

    with tempfile.TemporaryDirectory(prefix="ncro-bench-") as tmpdir:
        config_path = os.path.join(tmpdir, "ncro.toml")
        db_path = os.path.join(tmpdir, "routes.db")

        # Direct baseline: fresh TCP+TLS connection per request, no proxy.
        print("Measuring direct (no ncro)...", file=sys.stderr)
        direct_samples = measure_narinfo(
            hashes,
            UPSTREAM_URL,
            rounds=args.direct_rounds,
            verbose=args.verbose,
        )
        print_stats("direct", direct_samples)

        # Empty SQLite DB, one request per hash.
        # rounds=1 ensures each sample is a genuine first request for that
        # hash; repeating would hit ncro's in-process route+body cache :/
        write_ncro_config(config_path, db_path)
        print("\nStarting ncro (cold, empty DB)...", file=sys.stderr)
        proc = start_ncro(ncro_bin, config_path)
        if not wait_for_ncro():
            proc.kill()
            sys.exit("ncro did not become ready")

        try:
            print(
                "Measuring ncro cold (one request per hash, empty DB)...",
                file=sys.stderr,
            )
            cold_samples = measure_narinfo(
                hashes,
                NCRO_HOST,
                rounds=1,
                verbose=args.verbose,
            )
            print_stats("ncro cold", cold_samples)
        finally:
            stop_ncro(proc)

        route_count = count_sqlite_routes(db_path)
        if route_count is not None:
            print(f"  SQLite route entries after cold: {route_count}", file=sys.stderr)
            if route_count == 0:
                print(
                    "  WARNING: no routes written to SQLite. "
                    "Cold results may not represent a true cache miss.",
                    file=sys.stderr,
                )
        else:
            print("  (could not open SQLite DB to verify route count)", file=sys.stderr)

        # Process fully restarted
        print("\nRestarting ncro (warm, SQLite populated)...", file=sys.stderr)
        proc = start_ncro(ncro_bin, config_path)
        if not wait_for_ncro():
            proc.kill()
            sys.exit("ncro did not become ready (warm restart)")

        try:
            print(
                "Measuring ncro warm (one request per hash, SQLite route+body cached)...",
                file=sys.stderr,
            )
            warm_samples = measure_narinfo(
                hashes,
                NCRO_HOST,
                rounds=1,
                verbose=args.verbose,
            )
            print_stats("ncro warm", warm_samples)
        finally:
            stop_ncro(proc)

    # Sanity checks.
    cold_mean, _, cold_p50, _, _ = trimmed_stats(cold_samples)
    warm_mean, _, warm_p50, _, _ = trimmed_stats(warm_samples)

    if cold_p50 < 10.0:
        print(
            "\nWARNING: ncro cold p50 is suspiciously low (<10ms). "
            "SQLite may have had pre-existing routes. "
            "Verify the DB was empty before the cold run.",
            file=sys.stderr,
        )

    if warm_p50 > 50.0:
        print(
            "\nWARNING: ncro warm p50 is unexpectedly high (>50ms). "
            "Expect sub-millisecond latency when narinfo_bytes are served from SQLite.",
            file=sys.stderr,
        )

    direct_mean, direct_stdev, *_ = trimmed_stats(direct_samples)
    cold_mean, cold_stdev, *_ = trimmed_stats(cold_samples)
    warm_mean, warm_stdev, *_ = trimmed_stats(warm_samples)

    bars = [
        ("Direct", "no proxy", direct_mean, direct_stdev, "#2c2c2c"),
        ("ncro cold", "first request", cold_mean, cold_stdev, "#767676"),
        ("ncro warm", "SQLite body cache", warm_mean, warm_stdev, "#b8b8b8"),
    ]
    subtitle = f"{n_hashes} packages, trimmed mean +/-1 SD"

    out = Path(args.out)
    render_chart(bars, "narinfo lookup latency", subtitle, out)
    print(f"\nWrote {out}", file=sys.stderr)

    print("\n--- Results ---")
    rows = [
        ("Direct (no ncro)", direct_mean, direct_stdev, "baseline"),
        ("ncro cold", cold_mean, cold_stdev, speedup_text(cold_mean, direct_mean)),
        ("ncro warm", warm_mean, warm_stdev, speedup_text(warm_mean, direct_mean)),
    ]
    for label, mean, stdev, spd in rows:
        print(f"  {label:<22} {mean:7.1f} ms  s={stdev:<5.1f}  {spd}")


if __name__ == "__main__":
    main()
