.PHONY: fix check test cov type lint fmt

fmt:
	python -m black .
	python -m isort .

lint:
	flake8

type:
	mypy acmecli

test:
	pytest -q

cov:
	coverage run -m pytest -q >/dev/null 2>&1 || true; coverage report -m

fix: fmt lint  # format first, then show remaining lint if any

check: fmt lint type test cov
