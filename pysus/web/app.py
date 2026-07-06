"""PySUS Streamlit App — localhost visual interface for the PySUS package.

Run with:
    streamlit run pysus/web/app.py
    or
    pysus web
"""

import asyncio

import streamlit as st
from pysus.api.client import PySUS
from pysus.web.translations import t

LANGUAGES = {"English": "en", "Português": "pt"}
LANG_LABELS = {v: k for k, v in LANGUAGES.items()}

st.set_page_config(
    page_title="PySUS",
    page_icon=":hospital:",
    layout="wide",
)

st.markdown(
    """
    <style>
    [data-testid="stAppDeployButton"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner="loading datasets...", ttl=600)
def _load_catalog() -> None:
    async def _fetch():
        try:
            async with PySUS():
                return
        except Exception:  # noqa: B902
            _load_catalog.clear()
            return

    return asyncio.run(_fetch())


def _init_lang() -> None:
    if "lang" not in st.session_state:
        st.session_state.lang = "pt"


def _on_lang_change() -> None:
    label = st.session_state.get("_lang_select", "English")
    st.session_state.lang = LANGUAGES[label]


def _lang_selector() -> None:
    _init_lang()
    current_label = LANG_LABELS.get(st.session_state.lang, "Português")
    st.sidebar.selectbox(
        t("lang_label", st.session_state.lang),
        list(LANGUAGES.keys()),
        index=list(LANGUAGES.keys()).index(current_label),
        key="_lang_select",
        on_change=_on_lang_change,
    )


def home() -> None:
    _lang_selector()
    lang: str = st.session_state.lang

    # -- Hero --
    st.title(f":hospital: {t('intro_welcome', lang)}")
    st.markdown(t("intro_desc", lang))

    # -- Data Sources --
    st.subheader(f"📡  {t('intro_available', lang)}")
    sc1, sc2, sc3 = st.columns(3)
    for col, key in ((sc1, "ducklake"), (sc2, "ftp"), (sc3, "dadosgov")):
        with col:
            with st.container(border=True, height="stretch"):
                st.markdown(t(f"intro_source_{key}", lang))

    # -- Docs & GitHub --
    with st.container():
        st.markdown(t("intro_package", lang))

    with st.container():
        st.markdown(t("intro_github", lang))

    st.divider()

    # -- Databases --
    st.header(f"📊 {t('databases_title', lang)}")
    st.markdown(t("databases_desc", lang))

    databases = [
        ("SINAN", "sinan", "https://portalsinan.saude.gov.br/"),
        ("SINASC", "sinasc", "http://sinasc.saude.gov.br/"),
        ("SIM", "sim", "http://sim.saude.gov.br/default.asp"),
        ("SIH", "sih", "http://sihd.datasus.gov.br/principal/index.php"),
        ("SIA", "sia", "https://sia.datasus.gov.br/principal/index.php"),
        (
            "PNI",
            "pni",
            "https://sipni.datasus.gov.br/si-pni-web/faces/inicio.jsf",
        ),
        ("CNES", "cnes", "https://cnes.datasus.gov.br/"),
        ("CIHA", "ciha", "http://ciha.datasus.gov.br/CIHA/index.php"),
        ("IBGE", "ibge", "https://www.ibge.gov.br/"),
    ]

    for i in range(0, len(databases), 3):
        cols = st.columns(3)
        for j in range(3):
            if i + j >= len(databases):
                break
            name, db, url = databases[i + j]
            with cols[j]:
                with st.container(border=True, height="stretch"):
                    st.markdown(f"**{name}**")
                    st.caption(f"[{t('databases_source', lang)} ↗]({url})")
                    st.markdown(t(f"db_{db}_desc", lang))

    st.divider()


if __name__ == "__main__":
    _init_lang()
    _load_catalog()
    lang = st.session_state.lang

    home_page = st.Page(home, title=f"🏠️ {t('home_page', lang)}", default=True)
    client_page = st.Page("pages/1_client.py", title="📥️ Downloads")

    examples_page = st.Page("pages/2_examples.py", title="Examples")

    st.logo(
        "https://raw.githubusercontent.com/AlertaDengue/PySUS/"
        "db96a5ae94e899851490328ce784b3a5afd68a30/"
        "pysus/http/assets/logo_large.svg",
        icon_image=(
            "https://raw.githubusercontent.com/AlertaDengue/PySUS/"
            "db96a5ae94e899851490328ce784b3a5afd68a30/"
            "pysus/http/assets/logo.svg"
        ),
    )

    pg = st.navigation({"": [home_page, client_page], "docs": [examples_page]})
    pg.run()
