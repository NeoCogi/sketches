# Releasing sketches

The next release is **0.2.0**. Its incompatible API changes and migration notes
are recorded in [CHANGELOG.md](CHANGELOG.md). The manifest, lockfile and README
dependency examples must agree on the release version.

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

When ready to publish, date the changelog entry, commit it, rerun the package
and dry-run checks from the clean tree, then run:

```sh
cargo publish --locked
```

After successful publication, tag that commit as `v0.2.0` and push the release
branch and tag to the repository. Local preparation does not publish, tag or
push anything. Registry credentials should be configured outside the repository.
