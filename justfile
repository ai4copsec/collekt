default:
    just --list

# Install all dependencies (runtime + dev + test + docs + notebook groups) + git hooks
install: hooks
    uv sync --all-groups

# Install the pre-commit git hook (ruff, whitespace, and nbstripout on notebooks).
# Needed once per clone: git hooks live in .git/, which is not tracked.
hooks:
    uv sync --group dev --quiet
    uv run pre-commit install

# Run every pre-commit hook over the whole repo, not just staged files
hooks-all:
    uv run pre-commit run --all-files

# Format code with ruff
format:
    uv run ruff format src/ $([ -d tests ] && echo tests/)

# Lint (format first, then check)
lint: format
    uv run ruff check src/ $([ -d tests ] && echo tests/)

# Fix auto-fixable lint issues
fix:
    uv run ruff check --fix src/ $([ -d tests ] && echo tests/)

# Run tests
test:
    uv run pytest tests/ -v --timeout=120

# Run tests with coverage report
coverage:
    uv run pytest tests/ --cov=collekt --cov-report=term-missing --cov-report=html --timeout=120

# Build distribution wheel and sdist
build:
    uv build

# Check dependency tree for conflicts
check-deps:
    uv pip check

# Build docs locally for preview (CI builds & publishes them to GitHub Pages)
docs:
    rm -rf public docs/api
    uv run quartodoc build --config docs/_quarto.yml
    quarto render docs

# Fail if the working tree has uncommitted changes (the bump commit stages uv.lock)
check-clean:
    @git diff --quiet && git diff --cached --quiet || { echo "Working tree is dirty: commit or stash before bumping."; exit 1; }

# Lint + test (pre-release check, no build — build happens after version bump)
release-check: lint
    just test
    just check-deps

# Bump version (PATCH|MINOR|MAJOR): lint + test, then bump, tag and push
# `cz bump` re-locks uv.lock via its pre_bump_hook, so the commit is self-consistent
bump type="PATCH": release-check check-clean
    uv run cz bump --increment {{type}}
    just build
    git push origin main --follow-tags
