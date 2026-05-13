# Contributing to Basilisk

This document is the contribution guide for humans and automation.

## Table of Contents

- [1. Scope](#1-scope)
- [2. Development Setup](#2-development-setup)
- [3. Project Rules](#3-project-rules)
- [4. Coding Standards](#4-coding-standards)
- [5. Testing Requirements](#5-testing-requirements)
- [6. Documentation Requirements](#6-documentation-requirements)
- [7. Pull Request Checklist](#7-pull-request-checklist)

## 1. Scope

`rust-client` is the Rust client library for the **Milestone Basilisk** gateway and service bus. It provides:

- High-level `BasiliskClient` for end-to-end registration, authentication, and messaging
- `GatewayApiClient` for HTTP registry operations (register, deregister)
- `BusClient` for the TCP service bus (publish, subscribe, forward, request/response)

Contributions should stay within the client's domain: protocol handling, gateway API calls, and the public library surface.

## 2. Development Setup

### Prerequisites

- Rust toolchain (stable)
- `cargo`

### Common commands

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test -- --nocapture
```

### Run the example

```bash
cargo run --example basic_service_bus
```

## 3. Project Rules

- Keep the public API surface (`BasiliskClient`, `BusClient`, `GatewayApiClient`) backward compatible.
- Preserve existing `serde(rename = ...)` field names used by the wire protocol and gateway API.
- Keep service bus protocol framing (newline-delimited JSON) backward compatible when possible.
- Avoid introducing alternate transport or serialization mechanisms unless explicitly requested.

## 4. Coding Standards

- Keep modules focused; extract helpers when functions become hard to read.
- Prefer explicit names and small composable functions.
- Add Rustdoc comments to public types/functions where the behavior is not clear.
- Do not add comments that restate obvious code.
- Preserve existing public JSON field names and serde renames.

## 5. Testing Requirements

- Add or update tests for behavior changes.
- Favor top-level integration tests in `tests/` for public API behavior.
- Keep module-local unit tests only when private internals must be exercised.
- New protocol or routing changes should include success and error-path tests.

## 6. Documentation Requirements

- Update `README.md` when runtime behavior, configuration, API contracts, or flows change.
- For any new or changed public API, protocol message/field, or configuration value, documentation updates are mandatory in the same PR.
- Keep the documentation style consistent with existing project docs (section structure, writing tone, and snippet conventions) unless the PR is explicitly a documentation restructure.
- Keep examples executable and aligned with the current code.
- Do not include change logs in `README.md`; use commit/PR history for that.

## 7. Pull Request Checklist

Before opening a PR:

- [ ] Code is formatted (`cargo fmt --all`)
- [ ] Lints pass (`cargo clippy --all-targets --all-features -- -D warnings`)
- [ ] Tests pass (`cargo test -- --nocapture`)
- [ ] Tests cover public API behavior changes
- [ ] Documentation updated (`README.md`, inline rustdoc where needed)
- [ ] Documentation style is consistent with existing project documentation
- [ ] No unrelated file changes are included
