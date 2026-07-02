import typer
from pysus import __version__

app = typer.Typer(help="PySUS CLI")


@app.command()
def version():
    print(__version__)


@app.command()
def http(
    port: int = typer.Option(  # noqa: B008
        8501,
        "-p",
        "--port",
        help="Port to bind the server to",
    ),
):
    """Launch the local Streamlit visual interface."""
    try:
        import streamlit.web.bootstrap as bootstrap  # noqa
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # noqa
    except ImportError:
        raise ImportError(
            "The HTTP UI requires extra dependencies. "
            "Install them with: pip install pysus[http]"
        )
    import os
    import sys
    import webbrowser

    app_path = os.path.join(os.path.dirname(__file__), "..", "http", "app.py")
    app_path = os.path.abspath(app_path)

    from streamlit.web import cli as stcli

    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--server.address",
        "localhost",
    ]

    webbrowser.open(f"http://localhost:{port}")

    stcli.main()


if __name__ == "__main__":
    app()
