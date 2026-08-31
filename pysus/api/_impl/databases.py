"""High-level convenience functions for fetching Brazilian health data.

Each function wraps an asynchronous query/download pipeline and returns a
pandas DataFrame.  The available datasets cover disease notification (SINAN),
vital statistics (SINASC, SIM), hospital admissions (SIH), ambulatory care
(SIA), immunisation (PNI), census data (IBGE), health facilities (CNES),
hospitalisation records (CIHA), COVID-19, and all Saude open-data themes.
"""

from __future__ import annotations

import asyncio
import csv
import functools
import warnings
from typing import cast

import pandas as pd
from pysus.api import types
from pysus.api.errors import PySUSWarning
from tqdm.asyncio import tqdm

__all__ = [
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
    "list_files",
]

# ── Flat-API deprecation ─────────────────────────────────────────
# The top-level flat functions (``pysus.sinan``, ...) still work, but are
# deprecated in favor of the origin-namespaced API (``pysus.ftp.sinan``,
# ``pysus.dadosgov.sinan``, ``pysus.saude.sinan``).  Namespace wrappers
# suppress this warning via :class:`_suppress_flat_deprecation` so only a
# *direct* flat call is flagged.

_DEPRECATION_SUPPRESSED = False


class _suppress_flat_deprecation:
    """Context guard so namespace wrappers don't re-warn on the raw fn."""

    def __enter__(self) -> _suppress_flat_deprecation:
        global _DEPRECATION_SUPPRESSED  # noqa: PLW0603
        self._prior = _DEPRECATION_SUPPRESSED
        _DEPRECATION_SUPPRESSED = True
        return self

    def __exit__(self, *exc) -> None:
        global _DEPRECATION_SUPPRESSED  # noqa: PLW0603
        _DEPRECATION_SUPPRESSED = self._prior


def _deprecate_flat(fn):
    """Emit a deprecation warning for a direct flat ``pysus.<name>`` call."""

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        if not _DEPRECATION_SUPPRESSED:
            warnings.warn(
                f"pysus.{fn.__name__}() is deprecated and will be removed. "
                "Use the origin-namespaced API instead, e.g. "
                f"pysus.ftp.{fn.__name__}(...), "
                f"pysus.dadosgov.{fn.__name__}(...), or "
                f"pysus.saude.{fn.__name__}(...). Behavior is unchanged.",
                PySUSWarning,
                stacklevel=2,
            )
        return fn(*args, **kwargs)

    return wrapped


# ── Map canonical dataset names → Saude CKAN group slugs ─────────
_SAUDE_GROUP_MAP: dict[str, str] = {
    "ARBOVIROSES": "arboviroses",
    "ASSISTENCIASAUDE": "assistencia-a-saude",
    "ATENCAOPRIMARIA": "atencao-primaria",
    "BNAFAR": "assistencia-farmaceutica",
    "CIENCIATECNOLOGIA": "ciencia-tecnologia",
    "DIAGNOSTICOSTRATAMENTOS": "diagnosticos-e-tratamentos",
    "ECONOMIASAUDE": "economia-da-saude",
    "EDUCACAOSAUDE": "educacao-em-saude",
    "MACROSAUDE": "indicadores-de-saude",
    "OUVIDORIA": "ouvidoria",
    "OUTROSTEMAS": "outros-temas",
    "PDA": "pda",
    "PREVENCAOPROMOCAO": "prevencao-e-promocao-da-saude",
    "SISAGUA": "vigilancia-e-meio-ambiente",
    "SISVAN": "saude-indigena",
    "SAUDEINDIGENA": "saude-indigena",
    "VACINACAO": "vacinacao",
    "VIGILANCIAMEIOAMBIENTE": "vigilancia-e-meio-ambiente",
    "CNES": "cnes",
}


def _fetch_data(
    dataset: str,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    origin: str | None = None,
    source: str = "catalog",
    columns: list[str] | None = None,
    show_progress: bool = True,
    as_dataframe: bool = False,
    download: bool = True,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Query, download, and process Parquet files for a given dataset.

    Parameters
    ----------
    dataset : str
        Name of the dataset (e.g. ``"sinan"``, ``"sinasc"``).
    group : str, optional
        Group or disease code to filter by.
    state : str, optional
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int], optional
        Year or list of years to fetch.
    month : int | list[int], optional
        Month or list of months to fetch.
    origin : str, optional
        Origin mirror to serve from (``"FTP"``, ``"Saude"``,
        ``"DadosGov"``).  ``None`` uses the DuckLake catalog merging all
        origins.
    source : {"catalog", "origin"}
        Where to read from.  ``"catalog"`` (default) serves the DuckLake
        /S3 mirror; ``"origin"`` fetches directly from the origin server.
    columns : list[str], optional
        Subset of column names to keep in the final DataFrame.
    show_progress : bool, optional
        Whether to display a tqdm progress bar during download.
    as_dataframe : bool, optional
        Whether to concatenate and return a pandas DataFrame.
    download : bool, optional
        When ``False``, return the remote file paths that would be fetched
        without downloading them.  ``as_dataframe`` is ignored in that case.
        Defaults to ``True``.
    **kwargs
        Forwarded to :meth:`PySUS.read_parquet`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths to downloaded Parquet files (default) or a DataFrame.
    """
    from pysus.api._impl.source import fetch

    return fetch(
        dataset,
        origin=origin,
        source=source,
        group=group,
        state=state,
        year=year,
        month=month,
        columns=columns,
        show_progress=show_progress,
        as_dataframe=as_dataframe,
        download=download,
        **kwargs,
    )


async def _download_files(
    pysus,
    files,
    *,
    show_progress: bool = True,
    as_dataframe: bool = False,
    columns: list[str] | None = None,
    dataset: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Download remote files (throttled) and optionally return a DataFrame."""
    if not files:
        if as_dataframe:
            return pd.DataFrame()
        return cast(list[str], [])

    sem = asyncio.Semaphore(3)

    async def _throttled_download(f):
        async with sem:
            return await pysus.download(f)

    tasks = [_throttled_download(f) for f in files]

    if show_progress:
        downloaded_files = await tqdm.gather(
            *tasks,
            desc=f"Downloading {dataset or 'data'}",
            unit="file",
        )
    else:
        downloaded_files = await asyncio.gather(*tasks)

    paths: list[str] = [str(f.path) for f in downloaded_files]

    if as_dataframe:
        res = pysus.read_parquet(paths, **kwargs)
        df = res.df() if not isinstance(res, pd.DataFrame) else res
        if columns:
            df = df[[c for c in columns if c in df.columns]]
        return cast(pd.DataFrame, df)

    return paths


async def _fetch_ducklake(
    dataset: str,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    origin: str | None = None,
    columns: list[str] | None = None,
    show_progress: bool = True,
    as_dataframe: bool = False,
    download: bool = True,
    _bag: bool = False,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Query, download, and process Parquet files via DuckLake.

    ``origin`` filters the DuckLake catalog mirror by path prefix.  The
    origin → client mapping lives in :mod:`pysus.api._impl.source`.
    """
    from pysus.api._impl.source import _client_filter
    from pysus.api.client import PySUS

    async with PySUS() as pysus:
        client_filter = _client_filter(origin)

        files = await pysus.query(
            client=client_filter,
            dataset=dataset,
            group=group,
            state=state,
            year=year,
            month=month,
        )

        if not download:
            if _bag:
                return cast(list[str], files)
            return cast(list[str], [str(f.path) for f in files])

        return await _download_files(
            pysus,
            files,
            show_progress=show_progress,
            as_dataframe=as_dataframe,
            columns=columns,
            dataset=dataset,
            **kwargs,
        )


def _saude_csv_to_frame(path: str) -> pd.DataFrame | None:
    """Read a Saude CSV resource into a DataFrame, or ``None`` on failure.

    Saude resources are frequently Latin-1 (ISO-8859-1) even when advertised
    as UTF-8, so we sniff the delimiter and fall back across encodings.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read(4096)
        dialect = csv.Sniffer().sniff(text)
        sep = dialect.delimiter
    except Exception:  # noqa: BLE001
        sep = ","
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, sep=sep, low_memory=False, encoding=enc)
        except Exception:  # noqa: BLE001
            continue
    return None


async def _fetch_saude(
    dataset: str,
    group: str | None = None,
    columns: list[str] | None = None,
    show_progress: bool = True,
    as_dataframe: bool = False,
    download: bool = True,
) -> list[str] | pd.DataFrame:
    """Download data from the Saude portal (dadosabertos.saude.gov.br).

    The Saude portal organises data by CKAN groups (themes).  Each
    *dataset* name is mapped to one or more CKAN group slugs, and every
    downloadable resource under that group is fetched and optionally
    concatenated into a DataFrame.

    When ``download=False`` the CSV resource URLs are returned without
    downloading anything (``as_dataframe`` is ignored).
    """
    from pysus.api.client import PySUS

    dataset_upper = dataset.upper()
    ckan_group = _SAUDE_GROUP_MAP.get(dataset_upper, dataset.lower())

    async with PySUS() as pysus:
        saude = await pysus.get_saude()

        entries = await saude.list_datasets(group=ckan_group)
        if not entries:
            if as_dataframe and download:
                return pd.DataFrame()
            return cast(list[str], [])

        # Resolve each package's CSV resources exactly once.
        resources: list[tuple[str, str, str]] = []
        for entry in entries:
            try:
                pkg = await saude.fetch_dataset(entry.name)
                for res in pkg.resources:
                    if res.url and res.url.lower().endswith(".csv"):
                        resources.append((entry.name, res.id, res.url))
            except Exception:  # noqa: BLE001
                continue

        if not resources:
            if as_dataframe and download:
                return pd.DataFrame()
            return cast(list[str], [])

        if not download:
            return [url for _name, _rid, url in resources]

        dest = pysus.cachepath / "downloads" / "saude" / dataset.lower()
        dest.mkdir(parents=True, exist_ok=True)

        paths: list[str] = []
        iterator = resources
        if show_progress:
            iterator = tqdm(resources, desc=f"Downloading {dataset}", unit="ds")

        for name, resource_id, _url in iterator:
            try:
                p = await saude.download_resource(
                    name,
                    resource_id=resource_id,
                    dest_dir=dest,
                )
                paths.append(str(p))
            except Exception:  # noqa: BLE001
                continue

        if not paths:
            if as_dataframe:
                return pd.DataFrame()
            return cast(list[str], [])

        if as_dataframe:
            frames: list[pd.DataFrame] = []
            for p in paths:
                frame = _saude_csv_to_frame(p)
                if frame is not None:
                    frames.append(frame)
            if not frames:
                return pd.DataFrame()
            df = pd.concat(frames, ignore_index=True)
            if columns:
                df = df[[c for c in columns if c in df.columns]]
            return cast(pd.DataFrame, df)

        return paths


def sinan(
    disease: types.DatasetName,
    year: int | list[int],
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SINAN records for a given disease and year(s).

    SINAN is the Brazilian notifiable-disease information system.

    Parameters
    ----------
    disease : str
        Disease code (e.g. ``"DENG"`` for dengue, ``"ZIKA"``).
    year : int | list[int]
        Year or list of years to fetch.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sinan("DENG", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sinan",
        group=disease.upper(),
        year=year,
        **kwargs,
    )


def sinasc(
    state: types.State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SINASC birth certificates for a given state, year(s).

    SINASC is the Brazilian live birth information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sinasc("RJ", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sinasc",
        state=state.upper(),
        group=group,
        year=year,
        **kwargs,
    )


def sim(
    state: types.State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SIM mortality records for a given state, year(s).

    SIM is the Brazilian mortality information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sim("RJ", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sim",
        state=state.upper(),
        group=group,
        year=year,
        **kwargs,
    )


def sih(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SIH hospital admissions for a state, year, month.

    SIH is the Brazilian hospital admission information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sih("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sih",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def sia(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch SIA ambulatory care for a state, year, month.

    SIA is the Brazilian ambulatory care information system.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.sia("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="sia",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def pni(
    state: types.State,
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch PNI immunisation records for a given state, year(s).

    PNI is the Brazilian national immunisation programme.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.pni("RJ", 2020, as_dataframe=True)
    """
    return _fetch_data(
        dataset="pni",
        state=state.upper(),
        group=group,
        year=year,
        **kwargs,
    )


def ibge(
    year: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch IBGE census data for given year(s).

    IBGE provides census and demographic data.

    Parameters
    ----------
    year : int | list[int]
        Year or list of years to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.ibge(2020, as_dataframe=True)
    """
    return _fetch_data(dataset="ibge", group=group, year=year, **kwargs)


def cnes(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = None,
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch CNES health facilities for a state, year, month.

    CNES is the Brazilian registry of health-care facilities.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Additional grouping code.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.cnes("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="cnes",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


def ciha(
    state: types.State,
    year: int | list[int],
    month: int | list[int],
    group: str | None = "CIHA",
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch CIHA hospitalisation records for state, year, month.

    CIHA provides hospitalisation records.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation (e.g. ``"RJ"``).
    year : int | list[int]
        Year or list of years to fetch.
    month : int | list[int]
        Month or list of months to fetch.
    group : str, optional
        Grouping code.  Default is ``"CIHA"``.
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.ciha("RJ", 2020, 1, as_dataframe=True)
    """
    return _fetch_data(
        dataset="ciha",
        state=state.upper(),
        group=group,
        year=year,
        month=month,
        **kwargs,
    )


# ── DadosGov convenience function ────────────────────────────────


def covid19(
    **kwargs,
) -> list[str] | pd.DataFrame:
    """Fetch COVID-19 confirmed cases from dados.gov.br.

    Parameters
    ----------
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Returns
    -------
    list[str] | pd.DataFrame
        Paths or DataFrame.

    Examples
    --------
    >>> pysus.covid19(as_dataframe=True)
    """
    return _fetch_data(dataset="covid19", **kwargs)


# ── Saude portal convenience functions ───────────────────────────


def arboviroses(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch arboviroses (dengue, chikungunya, zika, yellow fever) from Saude.

    Parameters
    ----------
    **kwargs
        Forwarded to :func:`_fetch_data`.

    Examples
    --------
    >>> pysus.arboviroses(as_dataframe=True)
    """
    return _fetch_data(dataset="arboviroses", origin="Saude", **kwargs)


def assistencia_saude(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch hospital and health facility data from Saude.

    Examples
    --------
    >>> pysus.assistencia_saude(as_dataframe=True)
    """
    return _fetch_data(
        dataset="assistencia_saude",
        origin="Saude",
        **kwargs,
    )


def atencao_primaria(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch primary care data (Previne Brasil, SISAB) from Saude.

    Examples
    --------
    >>> pysus.atencao_primaria(as_dataframe=True)
    """
    return _fetch_data(
        dataset="atencao_primaria",
        origin="Saude",
        **kwargs,
    )


def bnafar(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch pharmaceutical assistance (Hórus stock) from Saude.

    Examples
    --------
    >>> pysus.bnafar(as_dataframe=True)
    """
    return _fetch_data(dataset="bnafar", origin="Saude", **kwargs)


def ciencia_tecnologia(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch science & technology data (Conitec, RIPSA) from Saude.

    Examples
    --------
    >>> pysus.ciencia_tecnologia(as_dataframe=True)
    """
    return _fetch_data(
        dataset="ciencia_tecnologia",
        origin="Saude",
        **kwargs,
    )


def diagnosticos_tratamentos(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch diagnostics & treatment protocols (PCDT) from Saude.

    Examples
    --------
    >>> pysus.diagnosticos_tratamentos(as_dataframe=True)
    """
    return _fetch_data(
        dataset="diagnosticos_tratamentos",
        origin="Saude",
        **kwargs,
    )


def economia_saude(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch health economics data (BPS, ApuraSUS, SIOPS) from Saude.

    Examples
    --------
    >>> pysus.economia_saude(as_dataframe=True)
    """
    return _fetch_data(
        dataset="economia_saude",
        origin="Saude",
        **kwargs,
    )


def educacao_saude(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch health education data (PVC) from Saude.

    Examples
    --------
    >>> pysus.educacao_saude(as_dataframe=True)
    """
    return _fetch_data(
        dataset="educacao_saude",
        origin="Saude",
        **kwargs,
    )


def macro_saude(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch macro-region and health region data from Saude.

    Examples
    --------
    >>> pysus.macro_saude(as_dataframe=True)
    """
    return _fetch_data(dataset="macro_saude", origin="Saude", **kwargs)


def ouvidoria(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch SUS ombudsman complaints from Saude.

    Examples
    --------
    >>> pysus.ouvidoria(as_dataframe=True)
    """
    return _fetch_data(dataset="ouvidoria", origin="Saude", **kwargs)


def outros_temas(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch miscellaneous CED coordination data from Saude.

    Examples
    --------
    >>> pysus.outros_temas(as_dataframe=True)
    """
    return _fetch_data(dataset="outros_temas", origin="Saude", **kwargs)


def pda(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch digital health and open data plan data from Saude.

    Examples
    --------
    >>> pysus.pda(as_dataframe=True)
    """
    return _fetch_data(dataset="pda", origin="Saude", **kwargs)


def prevencao_promocao(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch prevention & promotion (EPI distribution) from Saude.

    Examples
    --------
    >>> pysus.prevencao_promocao(as_dataframe=True)
    """
    return _fetch_data(
        dataset="prevencao_promocao",
        origin="Saude",
        **kwargs,
    )


def sisagua(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch water quality surveillance data from Saude.

    Examples
    --------
    >>> pysus.sisagua(as_dataframe=True)
    """
    return _fetch_data(dataset="sisagua", origin="Saude", **kwargs)


def sisvan(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch food & nutrition surveillance data from Saude.

    Examples
    --------
    >>> pysus.sisvan(as_dataframe=True)
    """
    return _fetch_data(dataset="sisvan", origin="Saude", **kwargs)


def saude_indigena(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch indigenous health data (Siasi/SasiSUS/Sesai) from Saude.

    Examples
    --------
    >>> pysus.saude_indigena(as_dataframe=True)
    """
    return _fetch_data(
        dataset="saude_indigena",
        origin="Saude",
        **kwargs,
    )


def vacinacao(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch vaccination data (PNI doses, ESAVI) from Saude.

    Examples
    --------
    >>> pysus.vacinacao(as_dataframe=True)
    """
    return _fetch_data(dataset="vacinacao", origin="Saude", **kwargs)


def vigilancia_meio_ambiente(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch environmental surveillance (SRAG, SIM, mpox) from Saude.

    Examples
    --------
    >>> pysus.vigilancia_meio_ambiente(as_dataframe=True)
    """
    return _fetch_data(
        dataset="vigilancia_meio_ambiente",
        origin="Saude",
        **kwargs,
    )


def saude_cnes(**kwargs) -> list[str] | pd.DataFrame:
    """Fetch CNES health-facility registers from Saude (CKAN resources).

    Unlike the FTP/DadosGov ``cnes`` fetcher (monthly state/year/month
    dumps), the Saude portal serves CNES as catalog resources
    (``/cnes/estabelecimentos``, ``/cnes/tipounidades``) fetched through
    ``_fetch_saude``.

    Examples
    --------
    >>> pysus.saude.cnes(download=False)
    >>> pysus.saude.cnes(as_dataframe=True)
    """
    return _fetch_data(dataset="cnes", origin="Saude", **kwargs)


def list_files(
    dataset: types.DatasetName,
    client: types.Origin | None = None,
    group: str | None = None,
    state: str | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """List catalog files filtered by client, group, state, year, and month.

    Queries the PySUS API metadata and returns a DataFrame with file data
    without downloading the actual files.

    Parameters
    ----------
    dataset : str
        Dataset name (e.g. ``"SINAN"``).
    client : str, optional
        Data source client to query.
    group : str, optional
        Group or disease code to filter by.
    state : str, optional
        Two-letter state abbreviation.
    year : int | list[int], optional
        Year or list of years to filter by.
    month : int | list[int], optional
        Month or list of months to filter by.
    **kwargs
        Forwarded to :meth:`PySUS.query`.

    Returns
    -------
    pd.DataFrame
        Columns: name, path, dataset, group, year, month, state, modify.

    Examples
    --------
    >>> pysus.list_files("SINAN", year=2020, state="RJ")
    """

    async def _list():
        from pysus.api.client import PySUS

        async with PySUS() as pysus:
            years = [year] if isinstance(year, int) else (year or [None])
            months = [month] if isinstance(month, int) else (month or [None])

            records = []
            for y in years:
                for m in months:
                    records.extend(
                        await pysus.query(
                            client=client,
                            dataset=dataset,
                            group=group,
                            state=state,
                            year=y,
                            month=m,
                        )
                    )

            return [
                {
                    "name": str(r.path).split("/")[-1],
                    "path": str(r.path),
                    "dataset": (r.dataset.name if r.dataset else None),
                    "group": (r.group.name if r.group else None),
                    "year": r.record.year,
                    "month": r.record.month,
                    "state": r.record.state,
                    "modify": r.record.origin_modified,
                }
                for r in records
            ]

    return pd.DataFrame(asyncio.run(_list()))


# ── Apply the flat-API deprecation wrapper to every public fetcher ─
# Namespaced wrappers (``_bind_origin``/``bind_list_files``) suppress the
# warning, so only direct ``pysus.<fetcher>(...)`` calls are flagged.
for _name in __all__:
    _obj = globals().get(_name)
    if callable(_obj):
        globals()[_name] = _deprecate_flat(_obj)
