#!/usr/bin/env python3
"""Generate conda v1 recipe (recipe.yaml) via grayskull.

Usage:
    python conda/generate_recipe.py

Runs ``grayskull pypi --strict-conda-forge --use-v1-format pysus``,
patches the result with project-specific overrides, and writes to
``conda/recipe/recipe.yaml``.
"""

import re
import subprocess
import sys
import tempfile
from pathlib import Path

from ruamel.yaml import YAML

RECIPE_PATH = Path(__file__).resolve().parent / "recipe" / "recipe.yaml"

MAINTAINERS: list[str] = [
    "fccoelho",
    "luabida",
    "esloch",
]

CONDA_NAME_MAP: dict[str, str] = {
    "wget": "python-wget",
    "dotenv": "python-dotenv",
    "python-duckdb": "duckdb",
}

SKIP_DEPS: set[str] = set()

CONDA_VERSION_OVERRIDES: dict[str, str] = {
    "duckdb-engine": ">=0.15.0",
}

yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)


def strip_post(spec: str) -> str:
    return re.sub(r"\.post\d+", "", spec)


def drop_upper_bounds(spec: str) -> str:
    return re.sub(r"\s*,?\s*<[=>]?\s*[\d.*]+", "", spec)


def drop_trailing_dotstar(spec: str) -> str:
    return re.sub(r"\.\*$", "", spec)


def clean_constraint(spec: str) -> str:
    spec = strip_post(spec)
    spec = drop_upper_bounds(spec)
    return spec


def patch_recipe(recipe: dict) -> dict:
    version = recipe["context"]["version"]
    recipe["context"] = {"version": version, "python_min": "3.11"}

    about = recipe["about"]
    about["homepage"] = "https://github.com/AlertaDengue/PySUS"
    about["documentation"] = "https://pysus.readthedocs.io/"
    about["repository"] = "https://github.com/AlertaDengue/PySUS"
    about["description"] = (
        "PySUS is a Python package for downloading, parsing, and analyzing "
        "Brazil's Public Health data (DATASUS). It provides tools for "
        "fetching data from various sources including FTP servers, DadosGov "
        "API, and DuckLake (S3-based catalog), also contains utilities for "
        "reading DBC/DBF file formats present in DATASUS."
    )

    recipe["extra"]["recipe-maintainers"] = MAINTAINERS

    recipe["requirements"]["host"] = [
        "python ${{ python_min }}.*" if d.startswith("python ") else d
        for d in recipe["requirements"]["host"]
    ]

    run_deps: list[str] = recipe["requirements"]["run"]
    new_run: list[str] = []
    for dep in run_deps:
        parts = dep.split(None, 1)
        name = parts[0]
        constraint = parts[1] if len(parts) > 1 else ""

        if name == "python":
            new_run.append("python >=${{ python_min }}")
            continue

        if name in SKIP_DEPS:
            continue

        if name in CONDA_NAME_MAP:
            name = CONDA_NAME_MAP[name]

        if name in CONDA_VERSION_OVERRIDES:
            constraint = CONDA_VERSION_OVERRIDES[name]
        elif constraint:
            constraint = clean_constraint(constraint)

        new_run.append(f"{name} {constraint}".strip())

    recipe["requirements"]["run"] = new_run

    recipe["tests"] = [
        {
            "python": {
                "imports": ["pysus"],
                "pip_check": True,
                "python_version": [
                    "${{ python_min }}.*",
                    "*",
                ],
            },
            "requirements": {"run": ["pip"]},
            "script": ["pysus --help"],
        }
    ]

    return recipe


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.run(
            [
                "grayskull",
                "pypi",
                "--strict-conda-forge",
                "--use-v1-format",
                "pysus",
                "-o",
                tmpdir,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            sys.exit(1)

        src = Path(tmpdir) / "pysus" / "recipe.yaml"
        recipe = yaml.load(src.read_text())
        recipe = patch_recipe(recipe)

    RECIPE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RECIPE_PATH, "w") as fh:
        yaml.dump(recipe, fh)
    print(f"Wrote {RECIPE_PATH}")


if __name__ == "__main__":
    main()
