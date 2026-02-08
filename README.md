Bash run command to test some Delta table "dt1" with latest version 43, with both the new and old delta-rs versions, for with / without files:

p="C:\Path\To\Table\dt1"; v=43; \
cargo run -p bench-new --release -- "$p" --version "$v" --mode with_files; \
cargo run -p bench-new --release -- "$p" --version "$v" --mode without_files; \
cargo run --release --manifest-path bench-old/Cargo.toml -- "$p" --version "$v" --mode with_files; \
cargo run --release --manifest-path bench-old/Cargo.toml -- "$p" --version "$v" --mode without_files
