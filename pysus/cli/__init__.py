import typer
from pysus import __version__
from pysus.tui.app import PySUS

app = typer.Typer(help="PySUS CLI")


@app.command()
def tui(
    lang: str = typer.Option("en", "-l", "--lang", help="Language (en, pt)"),
):
    app = PySUS(lang=lang)
    app.run()


@app.command()
def version():
    print(__version__)


if __name__ == "__main__":
    app()
