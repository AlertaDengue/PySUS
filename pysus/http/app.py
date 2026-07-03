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


@st.cache_data(show_spinner="loading datasets...", ttl=600)
def _load_catalog() -> None:
    async def _fetch():
        try:
            async with PySUS():
                return
        except Exception:
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

    st.title("Datasets")


if __name__ == "__main__":
    _init_lang()
    _load_catalog()
    lang = st.session_state.lang

    home_page = st.Page(home, title=f"🏠️ {t('home_page', lang)}", default=True)
    client_page = st.Page("pages/1_client.py", title="📥️ Downloads")

    examples_page = st.Page("pages/2_examples.py", title="Examples")

    st.logo(
        "https://raw.githubusercontent.com/AlertaDengue/PySUS/db96a5ae94e899851490328ce784b3a5afd68a30/pysus/http/assets/logo_large.svg",
        icon_image="https://raw.githubusercontent.com/AlertaDengue/PySUS/db96a5ae94e899851490328ce784b3a5afd68a30/pysus/http/assets/logo.svg",
    )

    pg = st.navigation({"": [home_page, client_page], "docs": [examples_page]})
    pg.run()
