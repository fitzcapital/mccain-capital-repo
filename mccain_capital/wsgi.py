"""WSGI entrypoint for production servers."""

from mccain_capital import create_app

app = create_app()
