"""Origin/source plumbing for the public API.

This module is the single internal primitive that decides **which mirror**
(``origin``) and **from where to read** (``source``) a dataset is served.

Two concepts, kept deliberately distinct:

- ``origin`` — the authoritative data source: ``FTP``, ``DadosGov`` or
  ``Saude``.  ``DuckLake`` is **not** an origin: it is the catalog/S3 cache
  that mirrors origin data.
- ``source`` — *where a call reads from*: ``"catalog"`` (the DuckLake cache,
  the default) or ``"origin"`` (directly from the origin server).

The mapping from an origin to the DuckLake catalog path prefix and to the
low-level client is defined once here and reused by every fetcher.
"""

from __future__ import annotations

import types as _pytypes
from typing import cast

import pandas as pd
from pysus.api import types

__all__ = [
    "fetch",
    "ORIGIN_CLIENT_MAP",
    "ORIGIN_PREFIXES",
    "APPLICABILITY",
    "origin_fetchers",
    "valid_origins",
]


# ── Canonical origin → DuckLake catalog path prefix ──────────────
# The catalog stores each origin's mirror under a distinct S3 prefix.
ORIGIN_PREFIXES: dict[str, str] = {
    "FTP": "public/data/ftp/",
    "DADOSGOV": "public/data/dadosgov/",
    "SAUDE": "public/data/saude/",
}


# ── Origin → low-level client name used by PySUS.download() ──────
ORIGIN_CLIENT_MAP: dict[str, str] = {
    "FTP": "ftp",
    "DADOSGOV": "dadosgov",
}

# Origins served through the DuckLake catalog (as origin mirrors).
CATALOG_ORIGINS: tuple[str, ...] = ("FTP", "DADOSGOV")

# The Saude origin pulls directly from the CKAN portal and is not backed
# by catalog mirror rows (the CLI already treats it as its own path).
SAUDE_ORIGIN: str = "SAUDE"


def valid_origins() -> tuple[str, ...]:
    """Return the canonical origin names (excluding DuckLake)."""
    return ("FTP", "DADOSGOV", "SAUDE")


# ── Origin × dataset applicability matrix ────────────────────────
# Which canonical fetchers each origin actually serves.  This is the single
# source of truth for (a) namespace scoping and (b) ``dadosgov.*`` omitting
# the datasets CKAN does not publish (rather than exposing-and-405).
#
# Values are the canonical function/dataset names exposed by each origin.
_APPLICABILITY: dict[str, frozenset[str]] = {
    "FTP": frozenset(
        {
            "sinan",
            "sinasc",
            "sim",
            "sih",
            "sia",
            "pni",
            "ibge",
            "cnes",
            "ciha",
            "covid19",
        }
    ),
    "DADOSGOV": frozenset(
        {
            "sinan",
            "sinasc",
            "sim",
            "cnes",
            "pni",
            "covid19",
        }
    ),
    "SAUDE": frozenset(
        {
            "arboviroses",
            "assistencia_saude",
            "atencao_primaria",
            "bnafar",
            "ciencia_tecnologia",
            "diagnosticos_tratamentos",
            "economia_saude",
            "educacao_saude",
            "macro_saude",
            "ouvidoria",
            "outros_temas",
            "pda",
            "prevencao_promocao",
            "sisagua",
            "sisvan",
            "saude_indigena",
            "vacinacao",
            "vigilancia_meio_ambiente",
        }
    ),
}

# Public, read-only view of the applicability matrix.
APPLICABILITY: dict[str, frozenset[str]] = dict(_APPLICABILITY)


def origin_fetchers(origin: str) -> frozenset[str]:
    """Return the canonical fetcher names applicable to an origin."""
    return _APPLICABILITY.get(origin.upper(), frozenset())


def _normalise_origin(origin: str | None) -> str | None:
    if origin is None:
        return None
    return origin.upper()


def _client_filter(origin: str | None) -> types.Origin | None:
    """Map a canonical origin to the DuckLake catalog ``client`` filter.

    ``None`` means *no filter* (the merged DuckLake catalog), which is the
    legacy flat-API behaviour.
    """
    from pysus.api.types import DADOSGOV, DUCKLAKE, FTP

    if origin is None:
        return None
    mapping = {
        "FTP": FTP,
        "DUCKLAKE": DUCKLAKE,
        "DADOSGOV": DADOSGOV,
    }
    return mapping.get(origin.upper())


async def _fetch_catalog(
    pysus,
    dataset: str,
    group: str | None,
    state: str | None,
    year: int | list[int] | None,
    month: int | list[int] | None,
    origin: str | None,
    columns: list[str] | None,
    show_progress: bool,
    as_dataframe: bool,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Serve a dataset from the catalog ``source="catalog"``.

    The Saude origin has **no DuckLake catalog mirror** — its datasets are
    served directly from the CKAN portal.  To preserve the historical
    ``_fetch_data(origin="Saude")`` behaviour (and keep the flat Saude
    functions working), ``origin="SAUDE"`` short-circuits to the direct
    portal fetch regardless of ``source``.
    """
    if origin is not None and origin.upper() == SAUDE_ORIGIN:
        return await _fetch_origin_direct(
            pysus,
            dataset,
            group,
            state,
            year,
            month,
            SAUDE_ORIGIN,
            columns,
            show_progress,
            as_dataframe,
            **kwargs,
        )

    from pysus.api._impl.databases import _fetch_ducklake

    return await _fetch_ducklake(
        dataset=dataset,
        group=group,
        state=state,
        year=year,
        month=month,
        origin=origin,
        columns=columns,
        show_progress=show_progress,
        as_dataframe=as_dataframe,
        **kwargs,
    )


async def _fetch_origin_direct(
    pysus,
    dataset: str,
    group: str | None,
    state: str | None,
    year: int | list[int] | None,
    month: int | list[int] | None,
    origin: str,
    columns: list[str] | None,
    show_progress: bool,
    as_dataframe: bool,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch directly from the origin server, bypassing the catalog mirror."""
    if origin == SAUDE_ORIGIN:
        from pysus.api._impl.databases import _fetch_saude

        return await _fetch_saude(
            dataset=dataset,
            group=group,
            columns=columns,
            show_progress=show_progress,
            as_dataframe=as_dataframe,
        )

    prefix = ORIGIN_PREFIXES.get(origin, "")
    client_name = ORIGIN_CLIENT_MAP.get(origin, "")
    if not client_name:
        from pysus.api.errors import ValidationError

        raise ValidationError(
            f"Unsupported origin for direct fetch: {origin!r}.",
            hint="Valid origins: 'FTP', 'DadosGov', 'Saude'.",
        )

    client_filter = _client_filter(origin)
    files = await pysus.query(
        client=client_filter,
        dataset=dataset,
        group=group,
        state=state,
        year=year,
        month=month,
    )
    files = [f for f in files if str(f.path).startswith(prefix)]

    if not files:
        if as_dataframe:
            return pd.DataFrame()
        return cast(list[str], [])

    from pysus.api._impl.databases import _download_files

    return await _download_files(
        pysus,
        files,
        show_progress=show_progress,
        as_dataframe=as_dataframe,
        columns=columns,
        dataset=dataset,
        **kwargs,
    )


def fetch(
    dataset: str,
    *,
    origin: str | None = None,
    source: str = "catalog",
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    columns: list[str] | None = None,
    show_progress: bool = True,
    as_dataframe: bool = False,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch a dataset from a given origin and source.

    Parameters
    ----------
    dataset : str
        Name of the dataset (e.g. ``"sinan"``).
    origin : str, optional
        Origin mirror to serve from: ``"FTP"``, ``"DadosGov"`` or
        ``"Saude"``.  ``None`` serves the merged DuckLake catalog.
    source : {"catalog", "origin"}
        Where to read from.  ``"catalog"`` (default) serves the DuckLake
        /S3 mirror; ``"origin"`` fetches directly from the origin server.
    group, state, year, month, columns, show_progress, as_dataframe
        Forwarded to the underlying fetch path.
    **kwargs
        Forwarded to the underlying fetch path (e.g. ``read_parquet`` opts).

    Returns
    -------
    list[str] | pd.DataFrame
        Paths to downloaded files or a concatenated DataFrame.
    """
    from pysus.api.client import _run_sync
    from pysus.api.errors import ValidationError
    from pysus.api.validate import validate_source

    source = validate_source(source)
    norm = _normalise_origin(origin)

    async def _run():
        from pysus.api.client import PySUS

        async with PySUS() as pysus:
            if source == "origin":
                if norm is None:
                    raise ValidationError(
                        "source='origin' requires an explicit origin.",
                        hint=(
                            "Pass origin='FTP', origin='DadosGov' or "
                            "origin='Saude'."
                        ),
                    )
                return await _fetch_origin_direct(
                    pysus,
                    dataset,
                    group,
                    state,
                    year,
                    month,
                    norm,
                    columns,
                    show_progress,
                    as_dataframe,
                    **kwargs,
                )
            return await _fetch_catalog(
                pysus,
                dataset,
                group,
                state,
                year,
                month,
                norm,
                columns,
                show_progress,
                as_dataframe,
                **kwargs,
            )

    return cast(list[str] | pd.DataFrame, _run_sync(_run()))


# ── Namespace factory ────────────────────────────────────────────


def _bind_origin(fn, origin: str):
    """Bind a flat fetcher to a fixed ``origin``.

    Returns a wrapper that:
    - rejects an explicit ``origin=`` keyword (it is fixed by the namespace);
    - injects the fixed ``origin`` into the underlying call;
    - forwards ``source=`` (``"catalog"`` default / ``"origin"``) through.

    The wrapper keeps the original name and docstring so ``help()`` and
    signature introspection remain useful.
    """
    import functools

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        if "origin" in kwargs:
            from pysus.api.errors import PySUSError

            raise PySUSError(
                f"{origin.lower()}.{fn.__name__} fixes origin to "
                f"{origin!r}; do not pass origin=.",
                hint="Use source='catalog' (default) or source='origin'.",
            )
        if "source" not in kwargs:
            kwargs["source"] = "catalog"
        # The Saude flat functions already hardcode ``origin="Saude"``
        # internally, so only inject origin for the catalog-backed origins.
        if origin.upper() != SAUDE_ORIGIN:
            kwargs["origin"] = origin
        from pysus.api._impl.databases import _suppress_flat_deprecation

        with _suppress_flat_deprecation():
            return fn(*args, **kwargs)

    wrapped.__name__ = fn.__name__
    _annotate_bound(wrapped, fn, origin)
    return wrapped


def _annotate_bound(wrapped, fn, origin: str) -> None:
    """Attach an origin-aware docstring to a bound namespace fetcher.

    The text points the user at the origin they are querying and the
    ``source`` parameter so ``pysus.ftp.sinan?`` / ``help()`` are
    immediately actionable.
    """
    if origin.upper() == SAUDE_ORIGIN:
        header = (
            f"This is the ``{origin.lower()}`` origin namespace version of "
            f"``pysus.{fn.__name__}`` — it reads the {_origin_desc(origin)}.\n"
            "The Saude portal has no catalog mirror, so every call queries "
            "the CKAN portal directly.\n\n"
        )
    else:
        header = (
            f"This is the ``{origin.lower()}`` origin namespace version of "
            f"``pysus.{fn.__name__}`` — it serves the {_origin_desc(origin)}.\n"
            "By default it reads the S3 catalog mirror (source='catalog'). "
            "Pass source='origin' to query the origin server directly.\n\n"
        )
    orig_doc = getattr(fn, "__doc__", "") or ""
    wrapped.__doc__ = header + orig_doc


def bind_list_files(origin: str):
    """Bind the flat ``list_files`` to a fixed origin client filter."""
    from pysus.api._impl.databases import list_files as _list_files
    from pysus.api.types import DADOSGOV, FTP

    client_lookup = {"FTP": FTP, "DADOSGOV": DADOSGOV}
    client = client_lookup.get(origin.upper())

    def bound(
        dataset,
        group=None,
        state=None,
        year=None,
        month=None,
        **kwargs,
    ) -> pd.DataFrame:
        from pysus.api._impl.databases import _suppress_flat_deprecation

        with _suppress_flat_deprecation():
            result = _list_files(
                dataset=dataset,
                client=client,
                group=group,
                state=state,
                year=year,
                month=month,
                **kwargs,
            )
        return result

    bound.__name__ = "list_files"
    bound.__doc__ = (
        f"List files available from the {origin} origin "
        "(mirror metadata) without downloading.\n\n"
        "Parameters\n----------\n"
        "dataset : str\n    Dataset name (e.g. ``'SINAN'``).\n"
        "group, state, year, month : optional\n    Filters.\n\n"
        "Returns\n-------\npd.DataFrame\n"
        "    Columns: name, path, dataset, group, year, month, state, modify."
    )
    return bound


def _origin_desc(origin: str) -> str:
    return {
        "FTP": "DATASUS FTP origin",
        "DADOSGOV": "DadosGov open-data origin",
        "SAUDE": "Saude portal (dadosabertos.saude.gov.br) origin",
    }.get(origin, origin)


def get_origin_meta(*, origin: str) -> dict[str, str | list[str]]:
    """Return static metadata about an origin namespace."""
    return {
        "origin": origin,
        "description": _origin_desc(origin),
        "fetchers": sorted(origin_fetchers(origin)),
    }


def build_origin_module(name: str, origin: str) -> _pytypes.SimpleNamespace:
    """Build an origin-namespaced module.

    Parameters
    ----------
    name : str
        Module name (e.g. ``"ftp"``).
    origin : str
        Canonical origin (``"FTP"``, ``"DADOSGOV"``, ``"SAUDE"``).

    Returns
    -------
    types.SimpleNamespace
        An object exposing the origin's fetchers, ``list_files``,
        ``info`` and ``get_origin_meta()``, with ``__all__``.
    """
    from pysus.api._impl import databases as _db

    origin_key = origin.upper()
    allowed = origin_fetchers(origin_key)

    ns: dict[str, object] = {}
    all_names: list[str] = []

    for fname in sorted(allowed):
        fn = getattr(_db, fname, None)
        if fn is None:
            continue
        ns[fname] = _bind_origin(fn, origin_key)
        all_names.append(fname)

    ns["list_files"] = bind_list_files(origin_key)
    all_names.append("list_files")

    def _info() -> None:
        """Print the datasets available from this origin.

        Example::

            >>> import pysus
            >>> pysus.ftp.info()

        Lists the databases this origin exposes (name + description),
        followed by a note with the cache path.
        """
        from pysus.api._impl._ui import _collect_datasets

        rows = [
            r
            for r in _collect_datasets()
            if r["origin"].lower() == origin_key.lower()
        ]
        if not rows:
            _origin_desc(origin_key)
            print(f"No datasets for origin {origin_key}.")
            return
        from pysus import CACHEPATH

        name_w = max(len(r["name"]) for r in rows)
        header = f"  {'Name':<{name_w}}  Description"
        print("  " + "-" * (len(header) - 2))
        print(header)
        print("  " + "-" * (len(header) - 2))
        for r in rows:
            print(f"  {r['name']:<{name_w}}  {r['description']}")
        print("  " + "-" * (len(header) - 2))
        print(
            f"\n  {origin_key} origin | {len(rows)} datasets | "
            f"Cache: {CACHEPATH}",
        )

    ns["info"] = _info
    all_names.append("info")

    def _get_origin_meta() -> dict[str, str | list[str]]:
        """Return metadata about this origin namespace.

        Returns a dict with ``origin`` (canonical name), ``description``
        and ``fetchers`` (the databases exposed on this namespace).

        Example::

            >>> import pysus
            >>> pysus.ftp.get_origin_meta()
        """
        return get_origin_meta(origin=origin_key)

    ns["get_origin_meta"] = _get_origin_meta
    all_names.append("get_origin_meta")

    ns["__all__"] = all_names
    return _pytypes.SimpleNamespace(**ns)


def install_origin_module(module, name: str, origin: str) -> None:
    """Populate a real Python module's globals with a built origin namespace.

    ``module`` should be the module object for ``pysus.<name>`` (e.g.
    ``sys.modules['pysus.ftp']``).  After this call, both
    ``import pysus.ftp`` and ``from pysus.ftp import sinan`` work.
    """
    ns = build_origin_module(name, origin)
    for key in ns.__all__:  # type: ignore[attr-defined]
        setattr(module, key, getattr(ns, key))
    module.__all__ = list(ns.__all__)  # type: ignore[attr-defined]
