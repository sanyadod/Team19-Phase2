.PHONY: fix check test cov type lint fmt test-selenium test-ui

fmt:
	python -m black .
	python -m isort .

lint:
	flake8

type:
	mypy acmecli

test:
	pytest -q

test-selenium:
	pytest tests/test_streamlit_ui.py -v

test-ui: test-selenium

cov:
	coverage run -m pytest -q >/dev/null 2>&1 || true; coverage report -m

fix: fmt lint  # format first, then show remaining lint if any

check: fmt lint type test cov
