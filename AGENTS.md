# Repository Guidelines

## Project Structure

- `src/`: top-level `resp-async` crate sources.
- `examples/`: examples for application developers
- `~/.cargo/registry/src`: depedencies source code path.

## Commands

- Build dev: `cargo build`
- Build release: `cargo build --release`
- Run integration tests: `cargo test`
- Lint: `cargo clippy`
- Format: `cargo format`

## Workflow Principles
- Always ensure format and lint is ok (`make format` and `make clippy`) before you finish your work.
- **NEVER** use git to discard or revert any changes not made by you - they are proposely changed by user and you should live with that.

## Engineering Rules

This project requires extremely high code quality and maintainability. Best engineering practices must be followed at all times.

The rules below are some typical principles. They are not exhaustive, and you must always use your best judgment to **write the cleanest code possible**.

### Core Principles: Simplicity & Readability

- Boring Code - Obvious, self-explanatory > clever, minimize cognitive load
- Single Responsibility - One function, one job
- Explicit over Implicit - Clear is better than concise
- Meaningful Abstractions - Only when they reduce cognitive load
- Keep DRY - Only if it does not conflict with the above principles

### Better Maintainability

- Keep the public API surface minimal; prefer reusing existing methods
  - Prefer `pub` over `pub(crate)` unless there is a strong reason; avoid hacky visibility levels
- Don't treat "looks similar" as "equivalent"
- Abstractions must be meaningful
- Keep certainty, single source of truth - e.g. don't introduce "optional" unless absolutely necessary
- Always add clear, concise and explicit doc comments for public APIs, complex logic, and non-obvious code

For non-refactor tasks:

- Prefer minimal, necessary, localized changes
- Follow existing code patterns, even if they are not ideal or party against our principles
  - Do your best to trade off between "ideal code", "consistent codebase", "only change what is necessary"
  - Point out any refactor chance you discovered after finishing your work, avoid changing them in the same work. Let human decides later.

### Naming Symbols

- Choosing the name that needs the least explanation, consider: verb clarity, noun specificity, context
- Name tests by behavior and expectation (e.g. `test_get_existing_key_valid_response`).

### Performance Requirements

- This project is a high performance application development framework.
- Reduce unnecessary allocations, copies, and data structure conversions.
