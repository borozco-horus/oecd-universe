UV_CACHE_DIR ?= /tmp/oecd-universe-uv-cache
RUN = UV_CACHE_DIR=$(UV_CACHE_DIR) uv run oecd-universe

.PHONY: discover estimate acquire normalize validate warehouse report mexico-report snapshot sync oecd-universe test lint format-check

discover:
	$(RUN) discover

estimate:
	$(RUN) estimate

acquire:
	$(RUN) acquire

normalize:
	$(RUN) normalize

validate:
	$(RUN) validate

warehouse:
	$(RUN) warehouse

report:
	$(RUN) report

mexico-report:
	$(RUN) mexico-report

snapshot:
	$(RUN) snapshot

sync:
	$(RUN) sync

oecd-universe:
	$(RUN) all

test:
	UV_CACHE_DIR=$(UV_CACHE_DIR) uv run pytest

lint:
	UV_CACHE_DIR=$(UV_CACHE_DIR) uv run ruff check .

format-check:
	UV_CACHE_DIR=$(UV_CACHE_DIR) uv run ruff format --check .

