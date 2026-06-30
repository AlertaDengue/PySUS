import typer
from pysus import __version__

app = typer.Typer(help="PySUS CLI")


@app.command()
def tui(
    lang: str = typer.Option(  # noqa
        "en",
        "-l",
        "--lang",
        help="Language (en, pt)",
    ),
):
    try:
        from pysus.tui.app import PySUS
    except ImportError:
        raise ImportError(
            "The TUI requires extra dependencies. "
            "Install them with: pip install pysus[tui]"
        )
    app = PySUS(lang=lang)
    app.run()


@app.command()
def version():
    print(__version__)


if __name__ == "__main__":
    app()
