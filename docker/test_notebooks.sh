#!/usr/bin/env bash
current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo ${current_dir}

pytest -vv --nbmake
