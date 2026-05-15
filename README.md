# memable

An embeddable durable execution engine for Rust.

Unlike platforms such as Temporal or Restate, memable is a **library** — it runs
inside your process with zero external dependencies. Workflows recover via
**key-based memoisation** rather than positional replay, so you can deploy new
code, fix bugs, and selectively re-execute steps without breaking in-flight
workflows.

## Target domains

- **Data integration & sync pipelines** — reliable multistep ETL with automatic
  retry and checkpointing.
- **AI agentic workflows** — durable orchestration of LLM calls, tool use, and
  branching agent logic.

## Status

Early development — the public API is not yet stable.

## Minimum Supported Rust Version

The MSRV is **1.85** (Rust 2024 edition).

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 licence, shall be
dual licensed as above, without any additional terms or conditions.
