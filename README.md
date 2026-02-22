# McCain Capital

Professional personal trading journal and analytics app (Flask + SQLite), packaged for local and container deployment.

## Architecture

- `/Users/kurtmccain/mccainc/mccain-capital-repo/app.py`
  - Thin compatibility entrypoint
- `/Users/kurtmccain/mccainc/mccain-capital-repo/mccain_capital/legacy_app.py`
  - Full existing business logic, templates, and data operations
- `/Users/kurtmccain/mccainc/mccain-capital-repo/mccain_capital/__init__.py`
  - App factory (`create_app`) and DB init
- `/Users/kurtmccain/mccainc/mccain-capital-repo/mccain_capital/wsgi.py`
  - Gunicorn WSGI entrypoint
- `/Users/kurtmccain/mccainc/mccain-capital-repo/mccain_capital/cli.py`
  - Local CLI runner
- `/Users/kurtmccain/mccainc/mccain-capital-repo/mccain_capital/routes/`
  - Route registration modules
- `/Users/kurtmccain/mccainc/mccain-capital-repo/mccain_capital/handlers/`
  - Split endpoint handler modules (delegating safely to legacy logic)
- `/Users/kurtmccain/mccainc/mccain-capital-repo/services/podman-compose.tailscale.yml`
  - App + Tailscale service stack

## Local Run

```bash
cd /Users/kurtmccain/mccainc/mccain-capital-repo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m mccain_capital.cli
```

## Podman Container Run

Build image:

```bash
cd /Users/kurtmccain/mccainc/mccain-capital-repo
podman build -t mccain-capital-app:latest -f Containerfile .
```

Run app:

```bash
podman rm -f mccain 2>/dev/null || true
podman run -d --name mccain -p 5001:5001 mccain-capital-app:latest
podman logs -f mccain
```

Open:

- `http://localhost:5001`

## Podman + Tailscale (Private VPN)

```bash
cd /Users/kurtmccain/mccainc/mccain-capital-repo
export TS_AUTHKEY=tskey-xxxxxxxx
podman compose -f services/podman-compose.tailscale.yml up -d --build
```

Check services:

```bash
podman compose -f services/podman-compose.tailscale.yml ps
podman logs -f mccain-capital-app
podman logs -f mccain-capital-tailscale
```

Use your node's Tailscale IP/hostname with port `5001` from devices on your tailnet.

## Notes

- UI theme is now deep-black futuristic across shared layout styles.
- OCR dependencies are lazy-loaded; non-OCR features continue working if OCR stack is unavailable.
- `pytesseract==0.3.13` is pinned for modern Python compatibility.
- Optional single-user login is available (recommended even on private VPN):
  - `APP_USERNAME=owner`
  - `APP_PASSWORD=your-password` or `APP_PASSWORD_HASH=<werkzeug hash>`
  - `SESSION_LIFETIME_MIN=720`
- Health check endpoint: `/healthz`
