# Releasing sketches

The next release is **0.3.0**. Its new RV module and migration notes
are recorded in [CHANGELOG.md](CHANGELOG.md). The manifest, lockfile and README
dependency examples must agree on the release version.

The published **0.2.0** archive came from commit `172bd79` and contains no RV
module. Its publication date, 2026-10-02, was confirmed from the crates.io index.
The 0.3.0 changelog entry remains **Unreleased** until actual publication.

Run the checks from the release checkout:

```sh
cargo fmt --all --check
cargo test --locked
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo package --locked --list
cargo package --locked
cargo publish --dry-run --locked
```

`cargo package` verifies the packaged crate by building it. Inspect the file
list as well: source, tests and reference data must be present, while repository
skills and audit reports must be absent. The manifest's explicit include list
defines the publication contents. A registry dry run additionally checks the
publication path and requires registry access.

Test the distributed files as well as the checkout. After packaging, unpack the
archive into a temporary directory and run its tests and target checks:

```sh
release_check_dir=$(mktemp -d)
tar -xzf target/package/sketches-0.3.0.crate -C "$release_check_dir"
CARGO_TARGET_DIR="$release_check_dir/target" cargo test --locked \
  --manifest-path "$release_check_dir/sketches-0.3.0/Cargo.toml"
CARGO_TARGET_DIR="$release_check_dir/target" cargo check --locked --all-targets \
  --manifest-path "$release_check_dir/sketches-0.3.0/Cargo.toml"
```

Keep any archive you intend to distribute outside the build target before
running `cargo clean`. A successful publication dry run ends with
`warning: aborting upload due to dry run`; no package upload occurs.

## Preparation verified on 2026-10-02

Verification used Rust/Cargo 1.98.1 on x86_64 Linux. The source checkout and the
unpacked 0.3.0 crate passed the following checks:

| Check | Result |
| --- | --- |
| Source and packaged test suites | 314 unit, 30 integration and 23 doctests passed in each |
| README Rust snippets against source and packaged libraries | All 15 passed in each |
| Repository formatting and all-target compilation | Passed |
| Strict all-target Clippy and warnings-denied rustdoc | Passed |
| Package file list and archive inspection | 52 files; all Rust targets and test reference data present; skills and audit reports excluded |
| Packaged all-target compilation | Passed |
| Registry-enabled `cargo publish --dry-run --locked` | Passed; upload aborted as expected |

The version, lockfile, README dependency examples and changelog agree on 0.3.0.
The new RV module preserves the previously published 0.2 public APIs. Preparation
has produced a verified local archive without publishing, tagging or pushing.

## Publication

When ready to publish, date the changelog entry, commit it, rerun the package
and dry-run checks from the clean tree, then run:

```sh
cargo publish --locked
```

After successful publication, tag that commit as `v0.3.0` and push the release
branch and tag to the repository. Local preparation does not publish, tag or
push anything. Registry credentials should be configured outside the repository.
