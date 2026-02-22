"""Compatibility entrypoint.

Keeps `python app.py` and legacy imports working while the app is packaged
under `mccain_capital`.
"""

from mccain_capital.wsgi import app


if __name__ == "__main__":
    from mccain_capital.cli import main

    main()
