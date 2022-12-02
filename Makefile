SHELL := /usr/bin/env bash
PYTHON := python
PYTHONPATH := ${PWD}


.PHONY: clean clean-test clean-pyc clean-build help
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT


help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

DOCKER = docker-compose -p pysus -f docker/docker-compose.yaml
SERVICE :=


#* Poetry
.PHONY: poetry-download
poetry-download:
	curl -sSL https://install.python-poetry.org | $(PYTHON) -

.PHONY: poetry-remove
poetry-remove:
	curl -sSL https://install.python-poetry.org | $(PYTHON) - --uninstall

#* Installation
.PHONY: install
install:
	poetry lock -n && poetry export --without-hashes > requirements.txt
	poetry build && poetry install

.PHONY: pre-commit-install
pre-commit-install:
	poetry run pre-commit install

#* Linting
.PHONY: check-codestyle
check-codestyle: ## check style with flake8
	# stop the build if there are Python syntax errors or undefined names
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	# exit-zero treats all errors as warnings. The GitHub editor is 127 chars wide
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

#* Docker basic
.PHONY: run-jupyter-pysus
run-jupyter-pysus: ## build and deploy all containers
	$(DOCKER) up -d --build

.PHONY: down-jupyter-pysus
down-jupyter-pysus: ## stop and remove containers for all services
	$(DOCKER) down -v --remove-orphans

#* Tests
.PHONY: test-jupyter-pysus
test-jupyter-pysus: ## run pytest for notebooks inside jupyter container
	$(DOCKER) exec -T jupyter bash /test_notebooks.sh

.PHONY: test
test: ## run tests quickly with the default Python
	poetry run pytest -vv pysus/tests/

coverage: ## check code coverage quickly with the default Python
	coverage run --source pysus/tests/ -m pytest
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

# Cleaning
.PHONY: clean
clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

.PHONY: clean-build
clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.cache' -exec rm -fr {} +
	find . -name '*.jupyter' -exec rm -fr {} +
	find . -name '*.local' -exec rm -fr {} +
	find . -name '*.mozilla' -exec rm -fr {} +
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.ipynb_checkpoints' -exec rm -rf {} +

.PHONY: clean-pyc
clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

.PHONY: clean-test
clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache
