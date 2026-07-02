import asyncio
from collections.abc import Coroutine
from typing import Any

import streamlit as st
from humanize import naturalsize

from pysus import CACHEPATH
from pysus.api.client import PySUS
from pysus.api.models import BaseRemoteFile
from pysus.http.translations import t

STATES = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
    "DF",
]

CLIENTS = {
    "ducklake": "ducklake_label",
    "ftp": "ftp_label",
    "dadosgov": "dadosgov_label",
}


def _lang() -> str:
    return st.session_state.get("lang", "en")


@st.cache_resource
def _get_orchestrator() -> PySUS:
    return PySUS()


def _run_async(coro: Coroutine[Any, Any, Any]) -> Any:
    return asyncio.run(coro)


def _clear_options_cache(client: str) -> None:
    for key in list(st.session_state.keys()):
        key = str(key)
        if key.startswith(f"_opts_{client}_"):
            del st.session_state[key]


def _cached_datasets(pysus: PySUS, client: str) -> list[Any]:
    cache_key = f"_datasets_{client}"
    if cache_key not in st.session_state:

        async def _load():
            if client == "ducklake":
                if pysus._ducklake is None:
                    return []
                return await pysus._ducklake.datasets()
            elif client == "ftp":
                ftp = await pysus.get_ftp()
                return await ftp.datasets()
            elif client == "dadosgov":
                if pysus._dadosgov is not None:
                    return await pysus._dadosgov.datasets()
                return []
            return []

        result = _run_async(_load())
        if not result:
            return []
        st.session_state[cache_key] = result
    return st.session_state[cache_key]


def _cached_options(
    client: str, datasets: list[Any], ds_name: str, opt_type: str
) -> list[Any]:
    cache_key = f"_opts_{client}_{ds_name}_{opt_type}"
    if cache_key not in st.session_state:
        if opt_type == "group":
            st.session_state[cache_key] = _get_group_options(client, datasets, ds_name)
        elif opt_type == "year":
            st.session_state[cache_key] = _get_year_options(client, datasets, ds_name)
        elif opt_type == "month":
            st.session_state[cache_key] = _get_month_options(client, datasets, ds_name)
    return st.session_state[cache_key]


def _get_group_options(client: str, datasets: list[Any], ds_name: str) -> list[str]:
    if not ds_name:
        return []
    target = next((d for d in datasets if d.name.upper() == ds_name.upper()), None)
    if target is None:
        return []

    if client == "ducklake":
        from sqlalchemy import select
        from pysus.api.ducklake.catalog.orm.dataset import Group as OrmGroup

        async def _fetch() -> list[str]:
            await target.adapter.connect()
            with target.adapter.get_session() as session:
                stmt = select(OrmGroup).filter(OrmGroup.dataset_id == target.id)
                orm_groups = session.scalars(stmt).all()
                return sorted(g.name for g in orm_groups)

        return _run_async(_fetch())

    if client == "ftp":
        return sorted(target.group_definitions.keys())

    if client == "dadosgov":
        return sorted(target.group_aliases.values())

    return []


def _get_year_options(client: str, datasets: list[Any], ds_name: str) -> list[int]:
    if not ds_name:
        return []
    if client != "ducklake":
        return []
    target = next((d for d in datasets if d.name.upper() == ds_name.upper()), None)
    if target is None:
        return []

    from sqlalchemy import distinct, select
    from pysus.api.ducklake.catalog.orm.dataset import File as OrmFile

    async def _fetch() -> list[int]:
        await target.adapter.connect()
        with target.adapter.get_session() as session:
            stmt = (
                select(distinct(OrmFile.year))
                .filter(OrmFile.dataset_id == target.id, OrmFile.year.isnot(None))
                .order_by(OrmFile.year)
            )
            return sorted(y for y in session.scalars(stmt).all())

    return _run_async(_fetch())


def _get_month_options(client: str, datasets: list[Any], ds_name: str) -> list[int]:
    if not ds_name:
        return []
    if client != "ducklake":
        return []
    target = next((d for d in datasets if d.name.upper() == ds_name.upper()), None)
    if target is None:
        return []

    from sqlalchemy import distinct, select
    from pysus.api.ducklake.catalog.orm.dataset import File as OrmFile

    async def _fetch() -> list[int]:
        await target.adapter.connect()
        with target.adapter.get_session() as session:
            stmt = (
                select(distinct(OrmFile.month))
                .filter(OrmFile.dataset_id == target.id, OrmFile.month.isnot(None))
                .order_by(OrmFile.month)
            )
            return sorted(m for m in session.scalars(stmt).all())

    return _run_async(_fetch())


def _render_year_filter(client: str, year_options: list[int]) -> list[int] | None:
    if year_options:
        return (
            st.multiselect(
                t("year", _lang()),
                year_options,
                placeholder=t("select_years", _lang()),
            )
            or None
        )
    if client != "ducklake":
        raw = st.text_input(t("year", _lang()), placeholder="2020, 2021, ...")
        if raw.strip():
            try:
                return [int(y.strip()) for y in raw.split(",") if y.strip()]
            except ValueError:
                st.error(t("invalid_year", _lang()))
                return None
    return None


def _render_month_filter(client: str, month_options: list[int]) -> list[int] | None:
    if month_options:
        return (
            st.multiselect(
                t("month", _lang()),
                month_options,
                placeholder=t("select_months", _lang()),
            )
            or None
        )
    if client != "ducklake":
        raw = st.text_input(t("month", _lang()), placeholder="1, 2, 3, ...")
        if raw.strip():
            try:
                return [int(m.strip()) for m in raw.split(",") if m.strip()]
            except ValueError:
                st.error(t("invalid_month", _lang()))
                return None
    return None


def _render_ducklake_filters(pysus: PySUS) -> None:
    if pysus._ducklake is None:
        with st.spinner(t("loading_catalog", _lang())):
            try:
                _run_async(pysus.get_ducklake())
            except Exception:
                st.warning(t("catalog_failed", _lang()))
                return
    datasets = _cached_datasets(pysus, "ducklake")
    if not datasets:
        st.warning(t("catalog_failed", _lang()))
        return
    ds_names = sorted(d.name.upper() for d in datasets)

    selected_ds = st.selectbox(
        t("dataset", _lang()),
        ds_names,
        index=None,
        placeholder=t("browser_choose", _lang()),
        key=f"_ds_ducklake",
    )
    ds_key = selected_ds or ""

    _prev = st.session_state.get("_prev_ds_ducklake")
    if selected_ds and selected_ds != _prev:
        st.session_state.pop("_query_results_ducklake", None)
        st.session_state.pop("_query_dataset_ducklake", None)
        st.session_state.pop("_query_params_ducklake", None)
        st.session_state["_prev_ds_ducklake"] = selected_ds

    if not selected_ds:
        return

    group_options = _cached_options("ducklake", datasets, ds_key, "group")
    year_options = _cached_options("ducklake", datasets, ds_key, "year")
    month_options = _cached_options("ducklake", datasets, ds_key, "month")

    col1, col2, col3 = st.columns(3)
    with col1:
        group = (
            st.multiselect(
                t("group", _lang()),
                group_options,
                placeholder=t("select_groups", _lang()),
            )
            or None
        )
    with col2:
        state = (
            st.multiselect(
                t("state", _lang()), STATES, placeholder=t("select_states", _lang())
            )
            or None
        )
    with col3:
        year = _render_year_filter("ducklake", year_options)

    month = _render_month_filter("ducklake", month_options)

    if st.button(t("fetch", _lang()), width="stretch"):
        _clear_options_cache("ducklake")
        _parse_and_query(pysus, "ducklake", selected_ds, group, state, year, month)


def _render_ftp_filters(pysus: PySUS) -> None:
    try:
        datasets = _cached_datasets(pysus, "ftp")
    except Exception as exc:
        st.warning(f"{t('ftp_failed', _lang())} {exc}")
        return
    if not datasets:
        return
    ds_names = [d.name for d in datasets]
    selected_ds = st.selectbox(
        t("dataset", _lang()),
        ds_names,
        index=None,
        placeholder=t("browser_choose", _lang()),
        key=f"_ds_ftp",
    )
    ds_key = selected_ds or ""

    _prev = st.session_state.get("_prev_ds_ftp")
    if selected_ds and selected_ds != _prev:
        st.session_state.pop("_query_results_ftp", None)
        st.session_state.pop("_query_dataset_ftp", None)
        st.session_state.pop("_query_params_ftp", None)
        st.session_state["_prev_ds_ftp"] = selected_ds

    if not selected_ds:
        return

    group_options = _cached_options("ftp", datasets, ds_key, "group")

    col1, col2, col3 = st.columns(3)
    with col1:
        group = (
            st.multiselect(
                t("group", _lang()),
                group_options,
                placeholder=t("select_groups", _lang()),
            )
            or None
        )
    with col2:
        state = (
            st.multiselect(
                t("state", _lang()), STATES, placeholder=t("select_states", _lang())
            )
            or None
        )
    with col3:
        year = _render_year_filter("ftp", [])

    month = _render_month_filter("ftp", [])

    if st.button(t("fetch", _lang()), width="stretch"):
        _parse_and_query(pysus, "ftp", selected_ds, group, state, year, month)


def _render_dadosgov_filters(pysus: PySUS) -> None:
    token = st.text_input(
        t("api_token", _lang()),
        type="password",
        placeholder=t("token_placeholder", _lang()),
    )

    if not token:
        st.info(t("token_required", _lang()))
        return

    if st.button(t("connect", _lang()), width="stretch"):
        with st.spinner(t("connecting_dadosgov", _lang())):
            _run_async(pysus.get_dadosgov(token))
            if "_datasets_dadosgov" in st.session_state:
                del st.session_state["_datasets_dadosgov"]

    if pysus._dadosgov is None:
        return

    datasets = _cached_datasets(pysus, "dadosgov")
    ds_names = [d.name for d in datasets]
    selected_ds = st.selectbox(
        t("dataset", _lang()),
        ds_names,
        index=None,
        placeholder=t("browser_choose", _lang()),
        key=f"_ds_dadosgov",
    )
    ds_key = selected_ds or ""

    _prev = st.session_state.get("_prev_ds_dadosgov")
    if selected_ds and selected_ds != _prev:
        st.session_state.pop("_query_results_dadosgov", None)
        st.session_state.pop("_query_dataset_dadosgov", None)
        st.session_state.pop("_query_params_dadosgov", None)
        st.session_state["_prev_ds_dadosgov"] = selected_ds

    if not selected_ds:
        return

    group_options = _cached_options("dadosgov", datasets, ds_key, "group")

    col1, col2, col3 = st.columns(3)
    with col1:
        group = (
            st.multiselect(
                t("group", _lang()),
                group_options,
                placeholder=t("select_groups", _lang()),
            )
            or None
        )
    with col2:
        state = (
            st.multiselect(
                t("state", _lang()), STATES, placeholder=t("select_states", _lang())
            )
            or None
        )
    with col3:
        year = _render_year_filter("dadosgov", [])

    month = _render_month_filter("dadosgov", [])

    if st.button(t("fetch", _lang()), width="stretch"):
        _parse_and_query(pysus, "dadosgov", selected_ds, group, state, year, month)


def _parse_and_query(
    pysus: PySUS,
    client: str,
    ds_name: str,
    group: list[str] | None,
    state: list[str] | None,
    year: list[int] | None,
    month: list[int] | None,
) -> None:
    if not ds_name:
        st.warning(t("select_dataset", _lang()))
        return

    groups = group or None
    states = state or None
    year_vals = year or None
    month_vals = month or None

    with st.spinner(t("querying", _lang(), client=client)):
        files = _run_async(
            _query_client(
                pysus,
                client,
                ds_name,
                groups,
                states,
                year_vals,
                month_vals,
            )
        )

    if not files:
        st.info(t("no_files", _lang()))
        return

    st.session_state[f"_query_results_{client}"] = files
    st.session_state[f"_query_dataset_{client}"] = ds_name
    st.session_state[f"_query_params_{client}"] = {
        "dataset": ds_name,
        "group": groups,
        "state": states,
        "year": year_vals,
        "month": month_vals,
    }
    st.success(t("files_found", _lang(), count=str(len(files))))


async def _ftp_dadosgov_search(
    target, groups, states, years, months
) -> list[BaseRemoteFile]:
    all_files = await target.search()
    if groups:
        all_files = [f for f in all_files if getattr(f.group, "name", None) in groups]
    if states:
        all_files = [f for f in all_files if getattr(f, "state", None) in states]
    if years:
        all_files = [f for f in all_files if getattr(f, "year", None) in years]
    if months:
        all_files = [f for f in all_files if getattr(f, "month", None) in months]
    return all_files


async def _query_client(
    pysus: PySUS,
    client: str,
    ds_name: str,
    groups: list[str] | None,
    states: list[str] | None,
    years: list[int] | None,
    months: list[int] | None,
) -> list[BaseRemoteFile]:
    if client == "ducklake":
        return await pysus.query(
            dataset=ds_name,
            group=groups,
            state=states,
            year=years,
            month=months,
        )

    if client == "ftp":
        ftp_client = await pysus.get_ftp()
        datasets = await ftp_client.datasets()
        target = next((d for d in datasets if d.name.upper() == ds_name.upper()), None)
        if target is None:
            return []
        try:
            return await _ftp_dadosgov_search(target, groups, states, years, months)
        except (ConnectionResetError, BrokenPipeError, OSError):
            await ftp_client.close()
            pysus._ftp = None
            ftp_client = await pysus.get_ftp()
            datasets = await ftp_client.datasets()
            target = next(
                (d for d in datasets if d.name.upper() == ds_name.upper()), None
            )
            if target is None:
                return []
            return await _ftp_dadosgov_search(target, groups, states, years, months)
    else:
        dadosgov_client = await pysus.get_dadosgov(None)
        datasets = await dadosgov_client.datasets()  # type: ignore[assignment]
        target = next((d for d in datasets if d.name.upper() == ds_name.upper()), None)
        if target is None:
            return []
        return await _ftp_dadosgov_search(target, groups, states, years, months)


def _build_file_row(f: BaseRemoteFile, queued: bool = False) -> dict[str, Any]:
    record = getattr(f, "record", None)
    year = record.year if record is not None else getattr(f, "year", None)
    month = record.month if record is not None else getattr(f, "month", None)
    state = record.state if record is not None else getattr(f, "state", None)
    if record is not None and record.group is not None:
        group_name = record.group.name
    elif f.group is not None:
        group_name = getattr(f.group, "name", "")
    else:
        group_name = ""

    return {
        "File": f.basename,
        "Size": naturalsize(f.size),
        "Year": year,
        "Month": month,
        "State": state,
        "Group": group_name,
        "Queued": "✅" if queued else "",
    }


def _show_results(pysus: PySUS, client: str) -> None:
    query_key = f"_query_results_{client}"
    queue_key = f"_download_queue_{client}"
    files = st.session_state.get(query_key, [])

    if not files:
        return

    if queue_key not in st.session_state:
        st.session_state[queue_key] = []

    download_queue = st.session_state[queue_key]
    queued_paths = {str(f.path) for f in download_queue}

    # --- Box 1: Query Results ---
    st.subheader(t("results_title", _lang(), count=str(len(files))))

    import pandas as pd

    df = pd.DataFrame([_build_file_row(f, str(f.path) in queued_paths) for f in files])

    event = st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
    )

    selected_indices = event.selection.get("rows", [])  # type: ignore[attr-defined]

    col1, col2 = st.columns([3, 1])
    with col2:
        if selected_indices and st.button(t("add_to_queue", _lang()), width="stretch"):
            added = 0
            for idx in selected_indices:
                f = files[idx]
                if str(f.path) not in queued_paths:
                    st.session_state[queue_key].append(f)
                    added += 1
            if added:
                st.rerun()

    # --- Box 2: Download Queue ---
    st.divider()
    st.subheader(t("queue_title", _lang(), count=str(len(download_queue))))

    if not download_queue:
        st.caption(t("queue_empty", _lang()))
        return

    queue_df = pd.DataFrame(
        [
            {"": idx, "File": f.basename, "Size": naturalsize(f.size)}
            for idx, f in enumerate(download_queue)
        ]
    ).set_index("")

    selection = st.dataframe(
        queue_df,
        width="stretch",
        height=min(35 * len(download_queue) + 38, 250),
        on_select="rerun",
        selection_mode="multi-row",
    )

    remove_indices = selection.selection.get("rows", [])  # type: ignore[attr-defined]

    dataset_name = st.session_state.get(f"_query_dataset_{client}", "")
    default_dir = str(CACHEPATH / "downloads" / client / (dataset_name or "data"))

    col1, col2, col3, col4 = st.columns([3, 1, 1, 2])
    with col1:
        download_dir = st.text_input(
            t("save_to", _lang()),
            value=default_dir,
            key=f"_dl_dir_{client}",
            label_visibility="collapsed",
            placeholder=t("download_dir_placeholder", _lang()),
        )
    with col2:
        if remove_indices and st.button(t("remove", _lang()), width="stretch"):
            for idx in sorted(remove_indices, reverse=True):
                st.session_state[queue_key].pop(idx)
            st.rerun()
    with col3:
        if st.button(t("clear", _lang()), width="stretch"):
            st.session_state[queue_key] = []
            st.rerun()
    with col4:
        if st.button(t("download", _lang()), width="stretch", type="primary"):
            _download_selected(pysus, client, download_queue, download_dir)


def _download_selected(
    pysus: PySUS,
    client: str,
    files: list[BaseRemoteFile],
    output_dir: str,
) -> None:
    queue_key = f"_download_queue_{client}"
    total = len(files)
    progress = st.progress(0, text=t("download_start", _lang()))

    async def _download():
        for i, f in enumerate(files):
            progress.progress(
                (i + 1) / total,
                text=t(
                    "downloading",
                    _lang(),
                    name=f.basename,
                    i=str(i + 1),
                    total=str(total),
                ),
            )
            try:
                await pysus.download(file=f)
            except Exception as exc:
                st.error(t("download_failed", _lang(), name=f.basename, error=str(exc)))

    _run_async(_download())
    progress.progress(1.0, text=t("download_done", _lang()))
    st.session_state[queue_key] = []
    st.success(t("download_success", _lang(), count=str(total), dir=output_dir))


def _build_query_code(client: str) -> str | None:
    params = st.session_state.get(f"_query_params_{client}")
    files = st.session_state.get(f"_query_results_{client}")
    if not params or not files:
        return None

    def _fmt_args(exclude_dataset: bool = False) -> str:
        lines = []
        for k in ("dataset", "group", "state", "year", "month"):
            if exclude_dataset and k == "dataset":
                continue
            v = params.get(k)
            if v:
                lines.append(f"        {k}={v!r},")
        return "\n".join(lines)

    if client == "ducklake":
        return (
            "from pysus.api.client import PySUS\n"
            "\n"
            "async with PySUS() as pysus:\n"
            "    files = await pysus.query(\n" + _fmt_args() + "\n    )\n"
            "    for f in files:\n"
            "        print(f.basename)\n"
            "        await pysus.download(file=f)\n"
        )

    if client == "ftp":
        return (
            "from pysus.api.client import PySUS\n"
            "\n"
            "async with PySUS() as pysus:\n"
            "    ftp = await pysus.get_ftp()\n"
            "    datasets = await ftp.datasets()\n"
            "    ds = next(d for d in datasets if d.name == "
            + repr(params["dataset"])
            + ")\n"
            "    files = await ds.search(\n"
            + _fmt_args(exclude_dataset=True)
            + "\n    )\n"
            "    for f in files:\n"
            "        print(f.basename)\n"
            "        await pysus.download(file=f)\n"
        )

    return (
        "from pysus.api.client import PySUS\n"
        "\n"
        "async with PySUS() as pysus:\n"
        "    dg = await pysus.get_dadosgov('YOUR_TOKEN')\n"
        "    datasets = await dg.datasets()\n"
        "    ds = next(d for d in datasets if d.name == "
        + repr(params["dataset"])
        + ")\n"
        "    files = await ds.search(\n" + _fmt_args(exclude_dataset=True) + "\n    )\n"
        "    for f in files:\n"
        "        print(f.basename)\n"
        "        await pysus.download(file=f)\n"
    )


# --- Page Layout ---

client_choice = st.segmented_control(
    t("source_label", _lang()),
    options=list(CLIENTS.keys()),
    format_func=lambda x: t(CLIENTS[x], _lang()),
    default="ducklake",
)

if client_choice is None:
    client_choice = "ducklake"

pysus = _get_orchestrator()

if client_choice == "ducklake":
    _render_ducklake_filters(pysus)
elif client_choice == "ftp":
    _render_ftp_filters(pysus)
elif client_choice == "dadosgov":
    _render_dadosgov_filters(pysus)

if st.session_state.get(f"_query_results_{client_choice}"):
    st.divider()
    _show_results(pysus, client_choice)

query_code = _build_query_code(client_choice)
if query_code:
    with st.expander(t("python_snippet", _lang())):
        st.code(query_code, language="python")
