"""Railway compatibility entrypoint.

Some Railway Python deployments default to ``gunicorn main:app``.
Expose the packaged Flask app here so platform defaults still boot.
"""
from mccain_capital.wsgi import app  # noqa: F401
