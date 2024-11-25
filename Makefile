SHELL := /usr/bin/env bash
PYTHON := python
PYTHONPATH := ${PWD}
ENVCREATE:=


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

DOCKER = docker compose -p pysus -f docker/docker-compose.yaml
SERVICE :=
SEMANTIC_RELEASE = npx --yes \
          -p semantic-release \
          -p conventional-changelog-conventionalcommits \
          -p "@semantic-release/commit-analyzer" \
          -p "@semantic-release/release-notes-generator" \
          -p "@semantic-release/changelog" \
          -p "@semantic-release/exec" \
          -p "@semantic-release/github" \
          -p "@semantic-release/git" \
          -p "semantic-release-replace-plugin" \
          semantic-release

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

.PHONY: test-pysus
test-pysus: ## run tests quickly with the default Python
	cp docs/source/**/*.ipynb pysus/Notebooks
	poetry run pytest -vv pysus/tests/

# RELEASE
# =======

.PHONY: release
release:
	$(SEMANTIC_RELEASE) --ci


.PHONY: release-dry
release-dry:
	$(SEMANTIC_RELEASE) --dry-run
