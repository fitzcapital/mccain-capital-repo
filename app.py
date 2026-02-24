"""Compatibility entrypoint.

Keeps `python app.py` working while the app is packaged under
`mccain_capital`.
"""

from mccain_capital.wsgi import app  # noqa: F401


if __name__ == "__main__":
    from mccain_capital.cli import main

    main()
