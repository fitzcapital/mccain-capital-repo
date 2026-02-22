# McCain Capital 🏛️📈

> A futuristic personal trading operating system built with Flask + SQLite.
> Log trades, review execution, track consistency, enforce risk, and learn faster.

## Why this app exists 🚀
Most trading journals track outcomes.
**McCain Capital tracks behavior + outcomes** so you can improve process, not just profit.

This is designed as a personal edge engine:
- 📊 Performance dashboard
- 🧠 Trade reviews + checklist scoring
- 🛡️ Risk lockout controls
- 📈 Setup/session analytics
- 📝 Journal workflow

## Product Showcase ✨

### Core modules
- 📅 **Trades**: import broker fills, manual entries, batch actions, review tags
- 📊 **Calendar Dashboard**: month heatmap + weekly/monthly/YTD rollups
- 🧠 **Analytics**: setup/session/time-of-day breakdowns + average checklist score
- 🛡️ **Risk Controls**: daily max-loss + optional lockout enforcement
- 📝 **Journal**: notes, mood, setup reflections
- 🧮 **Calculator**: options risk/reward planning

### Visual identity
- 🌌 Deep-black futuristic theme
- 💠 Cyan accent system for hierarchy and interactive states
- 📱 Mobile-aware layouts (quick dock + mobile trade cards)
- 🧊 Glass/elevated card UI for dashboard readability

## Feature highlights 🎯

### 1) Import-first trade workflow (built for statements)
- Supports broker-style and statement conversion flows
- Automatically creates review metadata for imported trades
- Auto-score is assigned so analytics works even without manual entry

### 2) Review & scoring engine
- Per-trade review fields:
  - setup tag
  - session tag
  - checklist score (0-100)
  - rule-break tags
  - review note
- Auto-score heuristic for imported trades
- `Avg Score` appears in analytics by setup/session/time bucket

### 3) Guardrails (discipline protection)
- Configure daily max loss
- Enable lockout when threshold is breached
- Trade-entry paths honor lockout state

### 4) Security + deploy readiness
- Optional single-user login
- Security headers (CSP, frame/content/referrer protection)
- Health check endpoint (`/healthz`)
- Podman-ready container deployment
- Tailscale private network support

## Architecture 🧱

- `app.py`: compatibility entrypoint
- `mccain_capital/legacy_app.py`: core app logic + templates
- `mccain_capital/__init__.py`: app factory + hooks
- `mccain_capital/routes/`: route registration
- `mccain_capital/handlers/`: endpoint handlers
- `mccain_capital/config.py`: runtime config profiles
- `services/podman-compose.tailscale.yml`: app + private VPN sidecar

## Quickstart ⚡

### Local
```bash
cd /Users/kurtmccain/mccainc/mccain-capital-repo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m mccain_capital.cli
```

### Container (Podman)
```bash
cd /Users/kurtmccain/mccainc/mccain-capital-repo
podman build -t mccain-capital-app:latest -f Containerfile .
podman rm -f mccain-capital-app 2>/dev/null || true
podman run -d --name mccain-capital-app -p 5001:5001 mccain-capital-app:latest
podman logs -f mccain-capital-app
```

Open: `http://localhost:5001`

## Private VPN mode (Tailscale + Podman) 🔐

```bash
cd /Users/kurtmccain/mccainc/mccain-capital-repo
export TS_AUTHKEY=tskey-xxxxxxxx
podman compose -f services/podman-compose.tailscale.yml up -d --build
podman compose -f services/podman-compose.tailscale.yml ps
```

## Environment variables 🛠️

- `SECRET_KEY`
- `DB_PATH`
- `UPLOAD_DIR`
- `BOOKS_DIR`
- `APP_USERNAME`
- `APP_PASSWORD` or `APP_PASSWORD_HASH`
- `SESSION_LIFETIME_MIN`
- `APP_ENV` (`dev` or `prod`)

## Roadmap ideas 🔭
- Score breakdown tooltip per trade
- Migration framework for schema evolution
- Automated weekly report generation
- Broker connectors for direct sync

## License / Usage
Personal project and portfolio build by Kurt McCain.
