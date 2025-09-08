# Repository Guidelines

## Project Structure & Module Organization
- Rust workspace managed by the root `Cargo.toml`.
- Applications: `application/xiu` (main server binary), plus utilities in `application/http-server` and `application/pprtmp`.
- Protocol crates: `protocol/{rtmp,rtsp/webrtc,hls,httpflv,mpegts}`.
- Library crates: `library/{bytesio,logger,streamhub,common,codec/h264,container/{flv,mpegts}}`.
- Config examples: `application/xiu/src/config/examples/`.
- Tests: integration tests under `protocol/hls/tests/`; unit tests live next to code (e.g., `protocol/httpflv/src/server_test.rs`).
- Docker assets: `docker/` (see `docker/start.sh`).

## Build, Test, and Development Commands
- Build workspace: `cargo build --workspace` or `make build`.
- Run server: `cargo run -p xiu -- -c application/xiu/src/config/examples/config.toml`.
- Switch manifests (local/online vendoring): `make local` or `make online`.
- Lint and quick fixes: `make check` (runs `cargo clippy --fix`).
- Format: `cargo fmt --all` (check only: `cargo fmt --all -- --check`).
- Test all crates: `cargo test --workspace` (verbose: `cargo test -- --nocapture`).

## Coding Style & Naming
- Rust 2018 edition; use `rustfmt` defaults (4‑space indent, max line width by tool).
- Naming: `snake_case` for functions/modules, `UpperCamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for consts.
- Prefer `anyhow::Result` for app-level errors; use custom errors where library APIs need typed errors.
- Logging via `log` macros; initialized by `application/xiu` using `env_logger_extend`.

## Testing Guidelines
- Place unit tests near code or in `tests/` for integration.
- Name tests descriptively (e.g., `server_test.rs`, `mod tests { ... }`).
- Run targeted tests: `cargo test -p rtmp` or `cargo test -p xiu <name>`.
- Add tests with new features and for bug fixes; avoid flaky network timing.

## Commit & Pull Request Guidelines
- Use conventional commits (e.g., `fix:`, `refactor:`, `feat:`) as seen in history.
- PRs must: describe the change and rationale, link issues, include repro/config used, and add tests/docs when applicable.
- CI requires `cargo build` and `cargo test` to pass.

## Security & Configuration Tips
- Do not hardcode secrets or tokens; prefer configs in `application/xiu/src/config/examples/*.toml` and environment variables.
- Validate exposed ports and `data_dir` paths in configs before running.
- To run in a container: `docker/start.sh` expects a config path argument.
