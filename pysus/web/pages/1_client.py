import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pandas as pd
import streamlit as st
from humanize import naturalsize
from pysus import CACHEPATH
from pysus.api.client import PySUS
from pysus.api.models import BaseRemoteFile
from pysus.web.translations import t

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
    return st.session_state.get("lang", "pt")


def _display_name(d: Any) -> str:
    """Build a selectbox label from a dataset's name and long_name."""
    name = d.name.upper() if hasattr(d, "name") else str(d)
    long_name = getattr(d, "long_name", "")
    return f"{name} — {long_name}" if long_name else name


def _ds_key(selected: str | None) -> str:
    """Extract the short dataset name from a display label."""
    if not selected:
        return ""
    return selected.split(" — ")[0]


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


def _cached_options_with_progress(
    client: str,
    datasets: list[Any],
    ds_name: str,
    opt_type: str,
) -> list[Any]:
    cache_key = f"_opts_{client}_{ds_name}_{opt_type}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    progress = st.progress(0, text=t("loading_catalog", _lang()))

    def _cb(dl: int, total: int) -> None:
        progress.progress(
            min(dl / max(total, 1), 1.0),
            text=t("loading_catalog", _lang()),
        )

    try:
        return _cached_options(
            client,
            datasets,
            ds_name,
            opt_type,
            callback=_cb,
        )
    finally:
        progress.empty()


def _cached_options(
    client: str,
    datasets: list[Any],
    ds_name: str,
    opt_type: str,
    callback: Callable[[int, int], None] | None = None,
) -> list[Any]:
    cache_key = f"_opts_{client}_{ds_name}_{opt_type}"
    if cache_key not in st.session_state:
        if opt_type == "group":
            st.session_state[cache_key] = _get_group_options(
                client, datasets, ds_name, callback=callback
            )
        elif opt_type == "year":
            st.session_state[cache_key] = _get_year_options(
                client, datasets, ds_name, callback=callback
            )
        elif opt_type == "month":
            st.session_state[cache_key] = _get_month_options(
                client, datasets, ds_name, callback=callback
            )
    return st.session_state[cache_key]


def _get_group_options(
    client: str,
    datasets: list[Any],
    ds_name: str,
    callback: Callable[[int, int], None] | None = None,
) -> list[str]:
    if not ds_name:
        return []
    target = next(
        (d for d in datasets if d.name.upper() == ds_name.upper()), None
    )
    if target is None:
        return []

    if client == "ducklake":
        from pysus.api.ducklake.catalog.orm.dataset import Group as OrmGroup
        from sqlalchemy import select

        async def _fetch() -> list[str]:
            await target.adapter.connect(callback=callback)
            with target.adapter.get_session() as session:
                stmt = select(OrmGroup).filter(OrmGroup.dataset_id == target.id)
                orm_groups = session.scalars(stmt).all()
                return sorted(g.name for g in orm_groups)

        try:
            return _run_async(_fetch())
        except Exception as exc:  # noqa: B902
            st.warning(t("catalog_query_failed", _lang(), error=str(exc)))
            return []

    if client == "ftp":
        return sorted(target.group_definitions.keys())

    if client == "dadosgov":
        return sorted(target.group_aliases.values())

    return []


def _get_year_options(
    client: str,
    datasets: list[Any],
    ds_name: str,
    callback: Callable[[int, int], None] | None = None,
) -> list[int]:
    if not ds_name:
        return []
    if client != "ducklake":
        return []
    target = next(
        (d for d in datasets if d.name.upper() == ds_name.upper()), None
    )
    if target is None:
        return []

    from pysus.api.ducklake.catalog.orm.dataset import File as OrmFile
    from sqlalchemy import distinct, select

    async def _fetch() -> list[int]:
        await target.adapter.connect(callback=callback)
        with target.adapter.get_session() as session:
            stmt = (
                select(distinct(OrmFile.year))
                .filter(
                    OrmFile.dataset_id == target.id, OrmFile.year.isnot(None)
                )
                .order_by(OrmFile.year)
            )
            return sorted(y for y in session.scalars(stmt).all())

    try:
        return _run_async(_fetch())
    except Exception as exc:  # noqa: B902
        st.warning(t("catalog_query_failed", _lang(), error=str(exc)))
        return []


def _get_month_options(
    client: str,
    datasets: list[Any],
    ds_name: str,
    callback: Callable[[int, int], None] | None = None,
) -> list[int]:
    if not ds_name:
        return []
    if client != "ducklake":
        return []
    target = next(
        (d for d in datasets if d.name.upper() == ds_name.upper()), None
    )
    if target is None:
        return []

    from pysus.api.ducklake.catalog.orm.dataset import File as OrmFile
    from sqlalchemy import distinct, select

    async def _fetch() -> list[int]:
        await target.adapter.connect(callback=callback)
        with target.adapter.get_session() as session:
            stmt = (
                select(distinct(OrmFile.month))
                .filter(
                    OrmFile.dataset_id == target.id, OrmFile.month.isnot(None)
                )
                .order_by(OrmFile.month)
            )
            return sorted(m for m in session.scalars(stmt).all())

    try:
        return _run_async(_fetch())
    except Exception as exc:  # noqa: B902
        st.warning(t("catalog_query_failed", _lang(), error=str(exc)))
        return []


def _render_year_filter(
    client: str, year_options: list[int]
) -> list[int] | None:
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


def _render_month_filter(
    client: str, month_options: list[int]
) -> list[int] | None:
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
        progress = st.progress(0, text=t("loading_catalog", _lang()))
        cum_dl = 0
        cum_total = 0
        last_total = -1
        last_dl = 0

        def _on_progress(dl: int, total: int) -> None:
            nonlocal cum_dl, cum_total, last_total, last_dl
            if last_total != total and last_total != -1:
                cum_dl += last_dl
                cum_total += last_total
                last_dl = 0
            last_total = total
            last_dl = dl
            cur_total = cum_total + total
            cur_dl = cum_dl + dl
            pct = cur_dl / max(cur_total, 1)
            progress.progress(min(pct, 1.0), text=t("loading_catalog", _lang()))

        try:
            _run_async(pysus.get_ducklake(callback=_on_progress))
            progress.empty()
        except Exception:  # noqa: B902
            progress.empty()
            st.warning(t("catalog_failed", _lang()))
            return
    datasets = _cached_datasets(pysus, "ducklake")
    if not datasets:
        st.warning(t("catalog_failed", _lang()))
        return
    ds_names = sorted(_display_name(d) for d in datasets)

    selected_ds = st.selectbox(
        t("dataset", _lang()),
        ds_names,
        index=None,
        placeholder=t("browser_choose", _lang()),
        key="_ds_ducklake",
    )
    ds_key = _ds_key(selected_ds)

    _prev = st.session_state.get("_prev_ds_ducklake")
    if selected_ds and selected_ds != _prev:
        st.session_state.pop("_query_results_ducklake", None)
        st.session_state.pop("_query_dataset_ducklake", None)
        st.session_state.pop("_query_params_ducklake", None)
        st.session_state["_prev_ds_ducklake"] = selected_ds

    if not selected_ds:
        return

    group_options = _cached_options_with_progress(
        "ducklake", datasets, ds_key, "group"
    )
    year_options = _cached_options_with_progress(
        "ducklake", datasets, ds_key, "year"
    )
    month_options = _cached_options_with_progress(
        "ducklake", datasets, ds_key, "month"
    )

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
                t("state", _lang()),
                STATES,
                placeholder=t("select_states", _lang()),
            )
            or None
        )
    with col3:
        year = _render_year_filter("ducklake", year_options)

    month = _render_month_filter("ducklake", month_options)

    if st.button(t("fetch", _lang()), width="stretch"):
        _clear_options_cache("ducklake")
        _parse_and_query(pysus, "ducklake", ds_key, group, state, year, month)


def _render_ftp_filters(pysus: PySUS) -> None:
    try:
        datasets = _cached_datasets(pysus, "ftp")
    except Exception as exc:  # noqa: B902
        st.warning(f"{t('ftp_failed', _lang())} {exc}")
        return
    if not datasets:
        return
    ds_names = [_display_name(d) for d in datasets]
    selected_ds = st.selectbox(
        t("dataset", _lang()),
        ds_names,
        index=None,
        placeholder=t("browser_choose", _lang()),
        key="_ds_ftp",
    )
    ds_key = _ds_key(selected_ds)

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
                t("state", _lang()),
                STATES,
                placeholder=t("select_states", _lang()),
            )
            or None
        )
    with col3:
        year = _render_year_filter("ftp", [])

    month = _render_month_filter("ftp", [])

    if st.button(t("fetch", _lang()), width="stretch"):
        _parse_and_query(pysus, "ftp", ds_key, group, state, year, month)


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
    ds_names = [_display_name(d) for d in datasets]
    selected_ds = st.selectbox(
        t("dataset", _lang()),
        ds_names,
        index=None,
        placeholder=t("browser_choose", _lang()),
        key="_ds_dadosgov",
    )
    ds_key = _ds_key(selected_ds)

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
                t("state", _lang()),
                STATES,
                placeholder=t("select_states", _lang()),
            )
            or None
        )
    with col3:
        year = _render_year_filter("dadosgov", [])

    month = _render_month_filter("dadosgov", [])

    if st.button(t("fetch", _lang()), width="stretch"):
        _parse_and_query(pysus, "dadosgov", ds_key, group, state, year, month)


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
        try:
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
        except Exception as exc:  # noqa: B902
            st.error(t("query_failed", _lang(), error=str(exc)))
            return

    if not files:
        st.info(t("no_files", _lang()))
        return

    st.session_state[f"_query_results_{client}"] = files
    st.session_state[f"_query_dataset_{client}"] = ds_name.lower()
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
        all_files = [
            f for f in all_files if getattr(f.group, "name", None) in groups
        ]
    if states:
        all_files = [
            f for f in all_files if getattr(f, "state", None) in states
        ]
    if years:
        all_files = [f for f in all_files if getattr(f, "year", None) in years]
    if months:
        all_files = [
            f for f in all_files if getattr(f, "month", None) in months
        ]
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
        target = next(
            (d for d in datasets if d.name.upper() == ds_name.upper()), None
        )
        if target is None:
            return []
        try:
            return await _ftp_dadosgov_search(
                target, groups, states, years, months
            )
        except OSError:
            await ftp_client.close()
            pysus._ftp = None
            ftp_client = await pysus.get_ftp()
            datasets = await ftp_client.datasets()
            target = next(
                (d for d in datasets if d.name.upper() == ds_name.upper()), None
            )
            if target is None:
                return []
            return await _ftp_dadosgov_search(
                target, groups, states, years, months
            )
    else:
        dadosgov_client = await pysus.get_dadosgov(None)
        datasets = await dadosgov_client.datasets()  # type: ignore[assignment]
        target = next(
            (d for d in datasets if d.name.upper() == ds_name.upper()), None
        )
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
        "Size (bytes)": int(f.size),
        "Size": naturalsize(int(f.size)),
        "Year": year,
        "Month": month,
        "State": state,
        "Group": group_name,
        "Queued": "✅" if queued else "",
    }


def _size_column_config() -> dict[str, Any]:
    return {
        "Size": st.column_config.TextColumn(t("size", _lang()), width="small"),
    }


def _native_dir_picker(title: str, initialdir: str) -> str:
    """Open a native directory picker dialog and return the selected path."""
    import platform
    import subprocess

    system = platform.system()

    if system == "Linux":
        for cmd in (
            [
                "zenity",
                "--file-selection",
                "--directory",
                f"--filename={initialdir}/",
                f"--title={title}",
            ],
            ["kdialog", "--getexistingdirectory", initialdir, "--title", title],
        ):
            try:
                r = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30
                )
                return r.stdout.strip()
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue

    elif system == "Windows":
        ps = f"""
Add-Type -AssemblyName System.Windows.Forms
$f = New-Object System.Windows.Forms.FolderBrowserDialog
$f.Description = '{title}'
$f.SelectedPath = '{initialdir}'
$f.ShowDialog() | Out-Null
$f.SelectedPath
"""
        r = subprocess.run(
            ["powershell", "-Command", ps],
            capture_output=True,
            text=True,
        )
        return r.stdout.strip()

    elif system == "Darwin":
        prompt_line = (
            'set f to choose folder with prompt "{}"'
            ' default location POSIX file "{}"'
        ).format(title, initialdir)
        applescript = (
            f'tell application "System Events"\n'
            f"    activate\n"
            f"    {prompt_line}\n"
            f"    POSIX path of f\n"
            f"end tell"
        )
        r = subprocess.run(
            ["osascript", "-e", applescript],
            capture_output=True,
            text=True,
        )
        return r.stdout.strip()

    return ""


def _show_results(pysus: PySUS, client: str) -> None:
    query_key = f"_query_results_{client}"
    queue_key = f"_download_queue_{client}"
    files = st.session_state.get(query_key, [])

    if not files:
        return

    if queue_key not in st.session_state:
        st.session_state[queue_key] = []

    raw_queue = st.session_state[queue_key]
    if raw_queue and not isinstance(raw_queue[0], int):
        st.session_state[queue_key] = []
        raw_queue = []

    queued_indices: list[int] = raw_queue
    download_queue = [files[i] for i in queued_indices]

    if msg := st.session_state.pop("_last_download_msg", None):
        st.success(msg)

    st.subheader(t("results_title", _lang(), count=str(len(files))))

    cache_key = f"_result_rows_{client}"
    cache_fp = (id(files), tuple(queued_indices))
    if st.session_state.get("_result_cache_fp") != cache_fp:
        st.session_state["_result_cache_fp"] = cache_fp
        st.session_state[cache_key] = [
            _build_file_row(f, i in queued_indices) for i, f in enumerate(files)
        ]
    df = pd.DataFrame(st.session_state[cache_key])
    for col in list(df.columns):
        if col != "File" and df[col].replace("", None).isna().all():
            df.drop(columns=[col], inplace=True)

    event = st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        column_config=_size_column_config(),
    )

    selected_indices = event.selection.get("rows", [])  # type: ignore

    col1, col2 = st.columns([3, 1])
    with col2:
        if selected_indices and st.button(
            t("add_to_queue", _lang()), width="stretch"
        ):
            new_indices = [
                i for i in selected_indices if i not in queued_indices
            ]
            if new_indices:
                st.session_state[queue_key] = sorted(
                    set(queued_indices) | set(new_indices)
                )
                st.rerun()

    st.divider()
    st.subheader(t("queue_title", _lang(), count=str(len(download_queue))))

    if not download_queue:
        st.caption(t("queue_empty", _lang()))
        return

    qcache_key = f"_queue_rows_{client}"
    qcache_fp = (id(files), tuple(queued_indices))
    if st.session_state.get("_q_cache_fp") != qcache_fp:
        st.session_state["_q_cache_fp"] = qcache_fp
        st.session_state[qcache_key] = [
            {
                "": idx,
                "File": f.basename,
                "Size (bytes)": int(f.size),
                "Size": naturalsize(int(f.size)),
            }
            for idx, f in enumerate(download_queue)
        ]
    queue_df = pd.DataFrame(st.session_state[qcache_key]).set_index("")

    selection = st.dataframe(
        queue_df,
        width="stretch",
        height=min(35 * len(download_queue) + 38, 250),
        on_select="rerun",
        selection_mode="multi-row",
        column_config=_size_column_config(),
    )

    remove_indices = selection.selection.get("rows", [])  # type: ignore

    dataset_name = st.session_state.get(f"_query_dataset_{client}", "").lower()
    default_dir = str(
        CACHEPATH / "downloads" / client / (dataset_name or "data")
    )

    dir_key = f"_dl_dir_{client}"
    if dir_key not in st.session_state:
        st.session_state[dir_key] = default_dir

    pending = st.session_state.pop("_dl_pending_" + client, None)
    if pending:
        st.session_state[dir_key] = pending

    col_dir, col_btn = st.columns([4, 1])
    with col_dir:
        st.text_input(
            t("save_to", _lang()),
            key=dir_key,
            label_visibility="collapsed",
        )
    with col_btn:
        if st.button(t("browse", _lang()), width="stretch"):
            folder = _native_dir_picker(
                title=t("browse_dir_title", _lang()),
                initialdir=st.session_state[dir_key],
            )
            if folder:
                st.session_state["_dl_pending_" + client] = folder
                st.rerun()
    col1, col2, col3 = st.columns(3)
    with col1:
        if remove_indices and st.button(t("remove", _lang()), width="stretch"):
            remove_targets = {queued_indices[i] for i in remove_indices}
            st.session_state[queue_key] = [
                i for i in queued_indices if i not in remove_targets
            ]
            st.rerun()
    with col2:
        if st.button(t("clear", _lang()), width="stretch"):
            st.session_state[queue_key] = []
            st.rerun()
    with col3:
        if st.button(t("download", _lang()), width="stretch", type="primary"):
            _download_selected(
                pysus, client, download_queue, st.session_state[dir_key]
            )
            st.rerun()


def _download_selected(
    pysus: PySUS,
    client: str,
    files: list[BaseRemoteFile],
    output_dir: str,
) -> None:
    queue_key = f"_download_queue_{client}"
    total = len(files)
    progress = st.progress(0, text=t("download_start", _lang()))
    failed: set[str] = set()

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
            except Exception as exc:  # noqa: B902
                failed.add(f.basename)
                st.error(
                    t(
                        "download_failed",
                        _lang(),
                        name=f.basename,
                        error=str(exc),
                    )
                )

    _run_async(_download())
    progress.empty()
    done = total - len(failed)

    st.session_state[queue_key] = [
        st.session_state[queue_key][i]
        for i, f in enumerate(files)
        if f.basename in failed
    ]
    st.session_state["_last_download_msg"] = t(
        "download_success", _lang(), count=str(done), dir=output_dir
    )


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
        "    files = await ds.search(\n"
        + _fmt_args(exclude_dataset=True)
        + "\n    )\n"
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
