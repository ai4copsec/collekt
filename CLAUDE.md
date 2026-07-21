# collekt

A library to facilitate spatio-temporal data collection across arbitrary
datasources.

This is an R&D Python package. Favor clarity over cleverness: small, well-named,
documented functions; no dead or experimental code left in `src/`.

## Environment & tooling

- Dependencies and the virtual environment (`.venv/`) are managed by **uv**; task
  shortcuts live in the **justfile**. Requires Python 3.13+.
- **Never run `pip install`.** Add a runtime dependency with `uv add <pkg>`, an
  optional one with `uv add --optional <extra> <pkg>`, and a tool with
  `uv add --group dev|test|docs <pkg>`. Commit the updated `uv.lock`.
- Run anything inside the project environment with `uv run <cmd>`.

## Common commands

- `just install` — install all dependencies (all groups + extras)
- `just lint` — format + lint with ruff
- `just test` — run the test suite
- `just docs` — build the Quarto documentation
- `just bump PATCH|MINOR|MAJOR` — lint + test + version bump + tag + push

## Definition of done

Before considering a change complete, run `just lint` and `just test`; both must
pass. Add or update a test for any behavior you change.

## Where code goes

- `src/collekt/` — importable library code (the public package)
- `tests/` — pytest tests · `docs/` — Quarto documentation
- Never commit secrets (keys, credentials) or datasets.

## General guidelines

1. Don't assume. Don't hide confusion. Surface tradeoffs. Ask if you are not sure.
2. Provide minimum code that solves the problem. Nothing speculative.
3. Touch only what you must. Clean up only your own mess.
4. Define success criteria. Loop until verified.

## Committing

When asked to, create a commit with all recent changes. Do not add the line about
the cocreation with Claude. Format the commit message following commitizen
recommendations.

Before committing, modify the CHANGELOG if it is a significant change (no linting
/ docs / minor stuff) and put it in the `[main]` tag. When asked to bump the
version number (see `just bump`), copy the entries under the new number.

## Docstrings

Documentation is built with [Quarto](https://quarto.org) and
[quartodoc](https://machow.github.io/quartodoc/).

All public docstrings must follow **Google style** and be compatible with this
configuration:

- Use `Args:`, `Returns:`, `Raises:`, `Example:` / `Examples:` section headers.
  **`Example:` must come before `Args:`** — quartodoc skips `Args:` if an
  `Example:` section follows it. Order: description → `Example:` → `Args:` →
  `Returns:` → `Raises:`.
- Code blocks inside docstrings must use fenced ` ```python ``` ` — **never**
  RST-style `.. code-block:: python` or bare `::` trailers.
- Always add a **blank line before** a fenced code block, and keep the fence
  **left-aligned** (not indented under the section header).
- Plain output blocks (non-Python) use ` ``` ``` ` without a language tag.
- Cross-references to other classes: use plain backticks `` `ClassName` `` (inline
  code). Do **not** use `:class:`X`` (RST/Sphinx) or `[X][]` (mkdocstrings) — both
  render as literal text in quartodoc.
- When you add a public function or class, list it in `docs/_quarto.yml` under
  `quartodoc.sections.contents` so it appears in the API reference.

## Repository layout

Keep the repository layout in `docs/modulemap.qmd` up to date after any structural
change (new or moved modules). Read it before reorganising the code.
