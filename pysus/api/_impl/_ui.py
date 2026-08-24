"""User-facing discovery helpers.

Provides ``info_table()`` and ``search()`` that give a non-technical
user a quick overview of what data is available and where to find it.

These are meant to be re-exported at the ``pysus`` namespace level.
"""

from __future__ import annotations


def _collect_datasets() -> list[dict[str, str]]:
    """Return a flat list of dicts describing every known dataset."""
    rows: list[dict[str, str]] = []

    try:
        from pysus.api.ftp.databases import AVAILABLE_DATABASES

        for ds_cls in AVAILABLE_DATABASES:
            name = ds_cls.__name__
            rows.append(
                {
                    "name": name,
                    "origin": "FTP",
                    "auth": "no",
                    "description": _FTP_DESC.get(name, name),
                },
            )
    except Exception:  # noqa: BLE001
        pass

    try:
        from pysus.api.saude.databases import DATASET_SPECS

        for spec in DATASET_SPECS:
            rows.append(
                {
                    "name": spec.name,
                    "origin": "Saude",
                    "auth": "no",
                    "description": _SAUDE_DESC.get(
                        spec.name,
                        spec.long_name,
                    ),
                },
            )
    except Exception:  # noqa: BLE001
        pass

    try:
        from pysus.api.dadosgov import databases as dg

        for dg_cls in dg.AVAILABLE_DATABASES:
            dg_name = dg_cls.__name__
            rows.append(
                {
                    "name": dg_name,
                    "origin": "DadosGov",
                    "auth": "yes",
                    "description": _DADOSGOV_DESC.get(dg_name, dg_name),
                },
            )
    except Exception:  # noqa: BLE001
        pass

    return rows


def info_table() -> None:
    """Print a table of all available datasets across every origin.

    This is the primary entry-point for a new user::

        import pysus
        pysus.info()
    """
    from pysus import CACHEPATH

    rows = _collect_datasets()
    if not rows:
        print("No datasets available.")
        return

    name_w = max(len(r["name"]) for r in rows)
    origin_w = max(len(r["origin"]) for r in rows)
    auth_w = max(len(r["auth"]) for r in rows)

    header = (
        f"  {'Name':<{name_w}}  {'Origin':<{origin_w}}  "
        f"{'Auth':<{auth_w}}  Description"
    )
    sep = "  " + "-" * (len(header) - 2)

    print(sep)
    print(header)
    print(sep)
    for r in rows:
        print(
            f"  {r['name']:<{name_w}}  {r['origin']:<{origin_w}}  "
            f"{r['auth']:<{auth_w}}  {r['description']}",
        )
    print(sep)
    print(
        f"\n  Total: {len(rows)} datasets | Cache: {CACHEPATH}",
    )


def search(
    query: str,
    *,
    origin: str | None = None,
) -> None:
    """Search datasets and column metadata by keyword.

    Parameters
    ----------
    query : str
        Search term (e.g. ``"dengue"``, ``"SIH"``, ``"peso"``).
    origin : str, optional
        Restrict to a specific origin (``"FTP"``, ``"Saude"``,
        ``"DadosGov"``).  ``None`` searches all.

    Examples
    --------
    >>> pysus.search("dengue")
    >>> pysus.search("SIH", origin="FTP")
    >>> pysus.search("peso")
    """
    import pandas as pd
    from pysus.api.columns import search_columns

    query_lower = query.lower()

    # --- Dataset matches ---
    datasets = _collect_datasets()
    ds_matches = [
        r
        for r in datasets
        if query_lower in r["name"].lower()
        or query_lower in r["description"].lower()
    ]
    if origin:
        ds_matches = [
            r for r in ds_matches if r["origin"].lower() == origin.lower()
        ]

    # --- Column matches ---
    col_matches = search_columns(query=query)

    # --- Print results ---
    if ds_matches:
        print(f"Datasets matching '{query}':")
        df = pd.DataFrame(ds_matches)
        if not df.empty:
            print(df.to_string(index=False))
    else:
        print(f"No datasets match '{query}'.")

    if col_matches:
        print(f"\nColumns matching '{query}':")
        for col in col_matches[:20]:
            desc = col.description_en or col.description
            print(f"  {col.name:30s}  {col.dataset:10s}" f"  {desc}")
        if len(col_matches) > 20:
            print(f"  ... and {len(col_matches) - 20} more")
    else:
        print(f"\nNo columns match '{query}'.")


_FTP_DESC: dict[str, str] = {
    "CIHA": "Hospital & ambulatory admission records",
    "CNES": "Health facility registry",
    "IBGEDATASUS": "Population & census data (IBGE)",
    "PNI": "National immunisation programme",
    "SIA": "Ambulatory care information system",
    "SIH": "Hospital admission information system",
    "SIM": "Mortality information system",
    "SINAN": "Notifiable disease information system",
    "SINASC": "Live birth information system",
}

_DADOSGOV_DESC: dict[str, str] = {
    "CNES": "Health facility registry",
    "PNI": "National immunisation programme",
    "SIM": "Mortality information system",
    "SINAN": "Notifiable disease information system",
    "SINASC": "Live birth information system",
    "COVID19": "Confirmed COVID-19 cases",
}

_SAUDE_DESC: dict[str, str] = {
    "ARBOVIROSES": "Arboviroses (dengue, chikungunya, zika, yellow fever)",
    "ASSISTENCIASAUDE": "Hospital and health facility data",
    "ATENCAOPRIMARIA": "Primary care (Previne Brasil, SISAB)",
    "BNAFAR": "Pharmaceutical assistance (Hórus medication stock)",
    "CIENCIATECNOLOGIA": "Science & technology (Conitec, RIPSA)",
    "DIAGNOSTICOSTRATAMENTOS": "Diagnostics & treatment protocols",
    "ECONOMIASAUDE": "Health economics (BPS, ApuraSUS, SIOPS)",
    "EDUCACAOSAUDE": "Health education (PVC)",
    "MACROSAUDE": "Macro-regions and health regions (MGDI)",
    "OUVIDORIA": "SUS ombudsman complaints",
    "OUTROSTEMAS": "Miscellaneous CED coordination data",
    "PDA": "Digital health and open data plan",
    "PREVENCAOPROMOCAO": "Prevention & promotion (EPI distribution)",
    "SISAGUA": "Water quality surveillance",
    "SISVAN": "Food & nutrition surveillance",
    "SAUDEINDIGENA": "Indigenous health (Siasi/SasiSUS/Sesai)",
    "VACINACAO": "Vaccination (PNI doses, ESAVI)",
    "VIGILANCIAMEIOAMBIENTE": "Environmental surveillance (SRAG, mpox)",
}
