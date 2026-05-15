# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Automatic retry with backoff for step closures that return `StepError::Retryable`.
- `RetryPolicy` type with `fixed()` and `exponential()` constructors, plus `with_max_delay()` builder method.
- `EngineBuilder::default_retry()` sets an engine-wide default retry policy.
- `StepBuilder::retry()` overrides the engine default for a specific step.
- `StepBuilder::no_retry()` disables retry for a specific step, overriding any engine default.
- `EngineError::RetriesExhausted` variant returned when all retry attempts fail.
- Dead-letter persistence: failed steps (both exhausted retries and permanent errors) are stored as `StepData::Failed` entries. Resume re-executes them with a fresh retry budget.
- `EngineError::InvalidKey` variant for key components containing the `/` delimiter.
- Runtime validation on all public entry points that accept workflow names, instance IDs, or step keys.

### Changed

- **Breaking:** `StepBuilder::run()` now requires `AsyncFnMut` instead of `AsyncFnOnce`. Step closures that capture from the workflow scope need `async move ||` with owned captures (clone `Arc`s before the closure).
- `Engine::register()` panics if the workflow name contains `/`.
- `Context::step()` and `Context::suspend()` panic if the key contains `/`.
- `Engine::invoke()`, `Engine::resume()`, `Engine::signal()`, and `Context::timer()` return `EngineError::InvalidKey` if any key component contains `/`.
- `EngineError::StepFailed` now includes a `retryable: bool` field, preserving whether the original `StepError` was `Retryable` or `Permanent`.
- `EngineError::step_failed()` constructor takes an additional `retryable: bool` parameter.
