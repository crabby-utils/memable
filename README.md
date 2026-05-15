# memable

[![Crates.io](https://img.shields.io/crates/v/memable.svg)](https://crates.io/crates/memable)
[![Documentation](https://docs.rs/memable/badge.svg)](https://docs.rs/memable)
[![CI](https://github.com/crabby-utils/memable/actions/workflows/ci.yml/badge.svg)](https://github.com/crabby-utils/memable/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/memable.svg)](https://crates.io/crates/memable)

An embeddable durable execution engine for Rust.

Platforms like Temporal and Restate are powerful, but they're platforms. They
bring their own runtime, their own infrastructure, their own opinions about how
you deploy. memable takes a different approach: it's a **library**. It runs
inside your process with no external dependencies.

Workflows recover through **key-based memoisation** rather than positional
replay. That means you can deploy new code, fix bugs, and selectively re-execute
steps without breaking in-flight workflows.

## Installation

```sh
cargo add memable
```

## What's it for?

- **Data integration and sync pipelines**. Reliable multistep ETL with automatic
  retry and checkpointing.
- **AI agentic workflows**. Durable orchestration of LLM calls, tool use, and
  branching agent logic.

## Documentation

[API docs on docs.rs](https://docs.rs/memable/0.1.0/memable/)

## Status

Early development. The public API is not yet stable.

## Minimum Supported Rust Version

1.85 (Rust 2024 edition).

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 licence, shall be
dual licensed as above, without any additional terms or conditions.
