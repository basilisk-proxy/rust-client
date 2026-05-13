# AGENTS.md

Operational guidance for human contributors and LLM/code agents working in this repository.

## 1. Purpose

Basilisk belongs to **Milestone**.

Work on the Basilisk Rust client safely without breaking core contracts:

- Gateway API registration and service lifecycle
- Service bus TCP connection and authentication flow
- Wire protocol compatibility (newline-delimited JSON)
- Public API stability for downstream consumers

## 2. Architecture Map

- `src/lib.rs`: crate root, public re-exports
- `src/basilisk_client.rs`: high-level `BasiliskClient` entry point
- `src/gateway_api.rs`: HTTP gateway registration/deregistration
- `src/bus_client.rs`: TCP service bus connection and message handling
- `src/protocol.rs`: wire message types and (de)serialization
- `src/error.rs`: `BasiliskError` and `Result` type alias
- `tests/tcp_bus_protocol_e2e.rs`: TCP protocol contract integration tests
- `tests/full_feature_e2e.rs`: end-to-end registration + bus flow tests

## 3. Non-Negotiable Contracts

1. **Register before connecting**: a service must register via the gateway API and receive an `instanceId` + `token` before authenticating the bus connection.
2. **Service bus framing**: all messages are newline-delimited JSON (`\n` terminated).
3. **Auth on connect**: the first message sent over the bus TCP connection must be an `auth` frame carrying the `instanceId` and `token`.
4. **Public API field names**: `serde(rename = ...)` annotations on protocol types must not change — external servers depend on them.
5. **Error transparency**: all fallible operations return `BasiliskError`; do not swallow errors silently.

## 4. Change Strategy

When making changes:

- Prefer small, focused commits.
- Preserve backward compatibility for public API fields and protocol names.
- If compatibility must change, update tests and `README.md` in the same change.
- Refactor large functions into helpers to reduce cognitive complexity.

## 5. Testing Policy

Minimum validation for non-trivial changes:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test -- --nocapture
```

If your change touches the protocol types, bus connection flow, or gateway API, add/update integration tests in `tests/`.

Integration tests require a live Milestone Basilisk server. Set `BASILISK_URL` and `BASILISK_BUS_PORT` environment variables (or accept the defaults) before running.

## 6. Documentation Policy

Update docs when the behavior changes:

- `README.md` for usage, configuration, API examples, and run behavior
- rustdoc comments for all public types and functions
- keep examples copy-paste runnable
- for any new or changed public type, protocol field, or client behavior, documentation updates are REQUIRED in the same change
- preserve existing documentation style and structure (headings, tone, and example format) unless a full docs restructuring is explicitly requested

Avoid including release/change-log style narrative in `README.md`.

## 7. Common Pitfalls

- Sending bus messages before a successful `auth` handshake
- Breaking `serde(rename = ...)` field names relied on by the Basilisk server
- Forgetting to terminate JSON frames with `\n`
- Swallowing `io::Error` or parse errors instead of mapping them to `BasiliskError`
- Adding blocking I/O calls where async is expected

## 8. Contribution Etiquette

- Do not rewrite unrelated code.
- Do not remove tests without replacement coverage.
- Keep naming explicit and domain-specific.
- If uncertain about behavior, add tests first, then implement.
