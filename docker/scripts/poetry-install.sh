#!/usr/bin/env bash

set -ex

poetry config virtualenvs.create false
poetry install --all-extras --with docs
