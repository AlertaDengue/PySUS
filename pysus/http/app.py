"""PySUS Streamlit App — localhost visual interface for the PySUS package.

Run with:
    streamlit run pysus/http/app.py
    or
    pysus http
"""

import asyncio

import streamlit as st

from pysus import __version__
from pysus.http.translations import t
from pysus.api.client import PySUS

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


@st.cache_data(show_spinner="Loading datasets...")
def load_catalog() -> None:
    async def _fetch():
        async with PySUS():
            return

    return asyncio.run(_fetch())


def _init_lang() -> None:
    if "lang" not in st.session_state:
        st.session_state.lang = "en"


def _on_lang_change() -> None:
    label = st.session_state.get("_lang_select", "English")
    st.session_state.lang = LANGUAGES[label]


def _lang_selector() -> None:
    _init_lang()
    current_label = LANG_LABELS.get(st.session_state.lang, "English")
    st.sidebar.selectbox(
        t("lang_label", st.session_state.lang),
        list(LANGUAGES.keys()),
        index=list(LANGUAGES.keys()).index(current_label),
        key="_lang_select",
        on_change=_on_lang_change,
    )


def home() -> None:
    catalog = load_catalog()
    st.session_state.catalog = catalog

    _lang_selector()
    lang: str = st.session_state.lang


if __name__ == "__main__":
    _init_lang()
    lang = st.session_state.lang

    home_page = st.Page(home, title=t("home_page", lang), default=True)
    datasets_page = st.Page("pages/1_ducklake.py", title="Datasets")
    ftp_page = st.Page("pages/2_ftp.py", title="FTP")
    dadosgov_page = st.Page("pages/3_dadosgov.py", title="DadosGov")

    st.logo(
        "https://raw.githubusercontent.com/luabida/PySUS/709a96d0cc9199894a2d9619a0189617d8f46a55/pysus/http/assets/logo_large.svg",
        icon_image="https://raw.githubusercontent.com/luabida/PySUS/7a3c210c80c47362d70996c0a005b60321f4bffa/pysus/http/assets/logo.svg",
    )

    pg = st.navigation([home_page, datasets_page, ftp_page, dadosgov_page])
    pg.run()
