# AGENTS.md - AI Agent Guide for atomic-lru

This document provides essential context and guidelines for AI agents working on the `asyncio-advanced-semaphores` codebase.

## Execute linting (style, type checking, import checking...) including some automatic fixes

Execute `make lint` to execute all these linting/fixing tools. Don't propose any changes if the linting fails (exit code different from 0).

## Execute tests

Execute `make test` to execute the tests. Don't propose any changes if the tests fail (exit code different from 0).

Notes:

- we use `pytest` for testing
- we don't want use mocks for testing (unless it's really necessary)
- we prefer function-based tests over class-based tests

## Coding Conventions

### Type Hints
- **Always** use type hints (Python 3.12+ features)
- Use generic types: `Storage[T]`, `Value[T]`
- Use `|` for union types (e.g., `int | None`, not `Optional[int]`)
- Use `Protocol` for structural subtyping (e.g., `Serializer`, `Deserializer`)

### Comments

- **Always** Python style comments when adding doctrings
- When adding examples in docstrings, use this format:

Example:
    ```python
    import os
    print(os.getcwd())
    ```

- Don't use `>>>` in docstrings
