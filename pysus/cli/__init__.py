import os
import sys
import webbrowser

import typer
from pysus import __version__

app = typer.Typer(help="PySUS CLI")

try:
    from .saude import app as saude_app

    app.add_typer(saude_app, name="saude")
except ImportError:
    pass

try:
    from .ftp import app as ftp_app

    app.add_typer(ftp_app, name="ftp")
except ImportError:
    pass

try:
    from .dadosgov import app as dadosgov_app

    app.add_typer(dadosgov_app, name="dadosgov")
except ImportError:
    pass

try:
    from .ducklake import app as ducklake_app

    app.add_typer(ducklake_app, name="ducklake")
except ImportError:
    pass


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
            from google.colab.output import (
                eval_js,  # type: ignore[import-untyped]
            )

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


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),  # noqa: B008
):
    """Search datasets across all origins (FTP, Saude, DadosGov)."""
    from difflib import get_close_matches

    from pysus import _DADOSGOV_DESCRIPTIONS, _FTP_DESCRIPTIONS

    query_lower = query.lower()
    rows: list[tuple[str, str, str, str]] = []

    # FTP
    try:
        from pysus.api.ftp.databases import AVAILABLE_DATABASES as FTP_DBS

        for ds_cls in FTP_DBS:
            name = ds_cls.__name__
            desc = _FTP_DESCRIPTIONS.get(name, "")
            if query_lower in name.lower() or query_lower in desc.lower():
                rows.append((name, "FTP", "no", desc))
    except Exception:  # noqa: BLE001
        pass

    # Saude
    try:
        from pysus.api.saude.databases import DATASET_SPECS

        for spec in DATASET_SPECS:
            if (
                query_lower in spec.name.lower()
                or query_lower in spec.long_name.lower()
            ):
                rows.append((spec.name, "Saude", "no", spec.long_name))
    except Exception:  # noqa: BLE001
        pass

    # DadosGov
    try:
        from pysus.api.dadosgov.databases import (
            AVAILABLE_DATABASES as DG_DATABASES,
        )

        for dg_cls in DG_DATABASES:
            name = dg_cls.__name__
            desc = _DADOSGOV_DESCRIPTIONS.get(name, "")
            if query_lower in name.lower() or query_lower in desc.lower():
                rows.append((name, "DadosGov", "yes", desc))
    except Exception:  # noqa: BLE001
        pass

    if not rows:
        # Collect all names for suggestions
        all_names: list[str] = []
        try:
            from pysus.api.ftp.databases import AVAILABLE_DATABASES as FTP_DBS

            all_names.extend(d.__name__ for d in FTP_DBS)
        except Exception:  # noqa: BLE001
            pass
        try:
            from pysus.api.dadosgov.databases import (
                AVAILABLE_DATABASES as DG_DATABASES2,
            )

            all_names.extend(d.__name__ for d in DG_DATABASES2)
        except Exception:  # noqa: BLE001
            pass

        close = get_close_matches(query, all_names, n=3, cutoff=0.4)
        if close:
            typer.echo(f"No datasets match '{query}'.")
            typer.echo(f"Did you mean: {', '.join(close)}?")
        else:
            typer.echo(f"No datasets match '{query}'.")
        raise typer.Exit(code=1)

    # Print table
    name_w = max(len(r[0]) for r in rows)
    origin_w = max(len(r[1]) for r in rows)
    auth_w = max(len(r[2]) for r in rows)
    header = (
        f"  {'Name':<{name_w}}  {'Origin':<{origin_w}}  "
        f"{'Auth':<{auth_w}}  Description"
    )
    sep = "  " + "-" * (len(header) - 2)

    typer.echo(sep)
    typer.echo(header)
    typer.echo(sep)
    for name, origin, auth, desc in rows:
        typer.echo(
            f"  {name:<{name_w}}  {origin:<{origin_w}}  "
            f"{auth:<{auth_w}}  {desc}"
        )
    typer.echo(sep)
    typer.echo(f"\n  Found: {len(rows)} dataset(s)")


@app.command()
def open(
    port: int = typer.Option(  # noqa: B008
        8501,
        "-p",
        "--port",
        help="Port to bind the server to",
    ),
):
    """Open PySUS in your web browser (alias for 'web')."""
    web(port=port, share=False)


@app.command()
def info():
    """Show a table of all available datasets."""
    from pysus import info as pysus_info

    pysus_info()
