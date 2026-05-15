# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `EngineError::InvalidKey` variant for key components containing the `/` delimiter.
- Runtime validation on all public entry points that accept workflow names, instance IDs, or step keys.

### Changed

- `Engine::register()` panics if the workflow name contains `/`.
- `Context::step()` and `Context::suspend()` panic if the key contains `/`.
- `Engine::invoke()`, `Engine::resume()`, `Engine::signal()`, and `Context::timer()` return `EngineError::InvalidKey` if any key component contains `/`.
- `EngineError::StepFailed` now includes a `retryable: bool` field, preserving whether the original `StepError` was `Retryable` or `Permanent`.
- `EngineError::step_failed()` constructor takes an additional `retryable: bool` parameter.
