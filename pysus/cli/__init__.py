import os
import sys
import webbrowser

import typer
from pysus import __version__

app = typer.Typer(help="PySUS CLI")


def _is_colab() -> bool:
    return "COLAB_RELEASE_TAG" in os.environ


@app.command()
def version():
    """Print the installed PySUS version."""
    print(__version__)


@app.command()
def web(
    port: int = typer.Option(  # noqa: B008
        8501,
        "-p",
        "--port",
        help="Port to bind the server to",
    ),
    share: bool = typer.Option(  # noqa: B008
        False,
        "--share",
        help="When running in Google Colab, print the proxied URL",
    ),
):
    """Launch the Streamlit web interface."""
    try:
        from streamlit.web import cli as stcli
    except ImportError as exc:
        raise ImportError(
            "The HTTP UI requires extra dependencies. "
            "Install them with: pip install pysus[web]"
        ) from exc

    app_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "web", "app.py")
    )

    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--server.address",
        "0.0.0.0",
    ]

    if _is_colab() and share:
        import threading

        def _run_streamlit():
            stcli.main()

        t = threading.Thread(target=_run_streamlit, daemon=True)
        t.start()

        import time

        time.sleep(3)

        try:
            from google.colab.output import eval_js

            url = eval_js(f"google.colab.kernel.proxyPort({port})")
            print(f"\nPySUS web interface running at:\n{url}\n")
        except ImportError:
            print(
                "\nGoogle Colab detected but google.colab is not available.\n"
                f"Open http://localhost:{port} manually.\n"
            )
        t.join()
    else:
        if not _is_colab():
            webbrowser.open(f"http://localhost:{port}")
        stcli.main()


if __name__ == "__main__":
    app()
