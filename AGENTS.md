# Repository Guidelines

## Project Structure & Module Organization
Runtime logic belongs in `src/`, organized by domain (`src/agents/`, `src/services/`, `src/utils/`) so ownership is obvious. Mirror that tree under `tests/` to keep fixtures and integration cases beside the code they validate. Specs, decision records, and onboarding notes live in `docs/`; helper automation goes in `scripts/`. Place datasets and reference assets in `assets/`, and keep environment samples in `config/.env.example` so secrets never land in git.

## Build, Test, and Development Commands
- `make install` — install Python and Node dependencies plus any toolchain pins.
- `make lint` — run `ruff`, `mypy`, and `eslint` with repo-standard settings.
- `make test` — execute all suites in `tests/` with coverage collection enabled.
- `make run` — launch the default entry point (`src/main.py`) using variables from `.env`.
Re-run `make install` after dependency changes so hooks stay synced.

## Coding Style & Naming Conventions
Use Python 3.11 defaults: four-space indentation, 100-character lines, `snake_case` modules, `PascalCase` classes, and `UPPER_SNAKE_CASE` constants. Front-end or TypeScript files follow two-space indentation and `camelCase` props. Always rely on `black`, `ruff`, and `prettier`—never edit generated code under `dist/`, `.cache/`, or `artifacts/`.

## Testing Guidelines
Write unit tests with `pytest`, naming files `test_<feature>.py` and mirroring the `src/` path. Integration and workflow scenarios should sit in `tests/integration/` and stub external APIs with `responses` or `vcrpy` to keep runs deterministic. Target ≥90% statement coverage (checked via `make test`) and always pair bug fixes with regression tests reproducing the failure.

## Commit & Pull Request Guidelines
Follow Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`) and keep each commit scoped to a single intent. Pull requests must explain motivation, list `make lint && make test` evidence, and link tracking issues. Attach screenshots or CLI logs for user-facing updates, mention migration steps, and request at least one peer review before merging; avoid force-pushing after reviews begin.

## Security & Configuration Tips
Never commit credentials—only `config/.env.example` with placeholder keys. Use local `.env` files for secrets and rotate CI values via the managed secret store. Before releases, run `make lint` and `pip-audit` to surface vulnerable dependencies, and confirm third-party tokens have the minimum required scopes.
