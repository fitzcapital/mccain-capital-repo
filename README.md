# McCain Capital 🏛️📈

<p align="center">
  <img src="docs/images/logo.png" alt="McCain Capital Logo" width="180" />
</p>

<p align="center">
  <b>Private Trading Workspace</b><br/>
  A personal trading operating system for execution, review, discipline, and performance growth.
</p>

---

## ✨ What This App Is

McCain Capital is a Flask + SQLite application that centralizes your trading workflow in one place.
It combines trade logging, journal discipline, risk controls, analytics, and planning tools into a single operating surface.

## 👤 Who It’s For

- Discretionary day traders and scalpers
- Traders who want structured review loops (not just raw P/L)
- Builders/learners who want a real, maintainable Python web app as a portfolio project

---

## 🚀 Core Capabilities

- 📊 **Dashboard Control Center**: live today/MTD/YTD metrics, calendar heatmap, projections
- 📋 **Trades Workspace**: manual entry, table paste, broker statement upload/import, review tags
- 📝 **Journal Workspace**: daily entries, linked-trade context, weekly review workflows
- 📈 **Analytics Workspace**: setup/session/hour edge diagnostics, expectancy + drawdown depth
- 🧮 **Calculator**: pre-trade stop/target/risk-reward planning
- 🎯 **Goals + Payouts**: discipline and payout-readiness tracking
- 🛡️ **Guardrails + Auth**: risk lockouts and access control support
- 🔔 **Operational Notifications**:
  - Sync success/fail
  - Guardrail lock/active state
  - Auto-sync missed/skipped warnings

---

## 🔁 Trading Workflow (Recommended)

1. Plan risk in **Calculator**
2. Execute and log in **Trades**
3. Document context in **Journal**
4. Review behavior and edge in **Analytics**
5. Monitor consistency and targets in **Dashboard / Goals / Payouts**

---

## 🧱 Architecture At A Glance

- Entrypoints: `app.py`, `mccain_capital/wsgi.py`, `mccain_capital/__init__.py`
- Main app surface: `mccain_capital/app_core.py` (legacy-compatible core)
- Routing: `mccain_capital/routes/`
- Handlers: `mccain_capital/handlers/`
- Services (domain logic): `mccain_capital/services/`
- Repositories (data access): `mccain_capital/repositories/`
- Templates: `mccain_capital/templates/`
- Static assets: `static/`
- Docs: `docs/`

### Data Flow

Browser request → Route → Handler → Service → Repository/SQLite → Template response

### Maintainability Notes (Recent Polishing)

- Dashboard UI extracted from inline core string into `mccain_capital/templates/dashboard.html`
- Auth/Calculator screens extracted into templates:
  - `mccain_capital/templates/setup_login.html`
  - `mccain_capital/templates/login.html`
  - `mccain_capital/templates/calculator.html`
- Shared system status + alert strip centralized in:
  - `mccain_capital/services/ui.py`
  - `mccain_capital/templates/base.html`

---

## ⚡ Quickstart (Local)

```bash
cd /mccain-capital-repo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m mccain_capital.cli
```

Open: `http://localhost:5001`

Optional explicit migration run:

```bash
python migrate.py
```

## 🐳 Quickstart (Podman)

```bash
cd /mccain-capital-repo
podman build -t mccain-capital-app:latest -f Containerfile .
podman rm -f mccain-capital-app 2>/dev/null || true
podman volume create mccain-capital-data
podman run -d --name mccain-capital-app -p 5001:5001 \
  -e DB_PATH=/data/journal.db \
  -e UPLOAD_DIR=/data/uploads \
  -e BOOKS_DIR=/data/books \
  -v mccain-capital-data:/data \
  mccain-capital-app:latest
podman logs -f mccain-capital-app
```

Open: `http://localhost:5001`

### Data Persistence

With the `mccain-capital-data` volume, all app data persists across rebuilds/restarts:

- journal/trades database: `/data/journal.db`
- uploads/debug artifacts: `/data/uploads`
- books/library files: `/data/books`

---

## 🖼️ Screenshots

### 💻 Desktop

#### 📊 Dashboard
![Desktop Dashboard](docs/images/desktop-dashboard.png)

#### 📋 Trades
![Desktop Trades](docs/images/desktop-trades.png)

#### 📝 Journal
![Desktop Journal](docs/images/desktop-journal.png)

#### 🧮 Calculator
![Desktop Calculator](docs/images/desktop-calculator.png)

#### 📈 Analytics
![Desktop Analytics](docs/images/desktop-analytics.png)

### 📱 Mobile

#### 📊 Dashboard
![Mobile Dashboard](docs/images/mobile-dashboard.png)

#### 📋 Trades
![Mobile Trades](docs/images/mobile-trades.png)

#### 📝 Journal
![Mobile Journal](docs/images/mobile-journal.png)

#### 🧮 Calculator
![Mobile Calculator](docs/images/mobile-calculator.png)

#### 📈 Analytics
![Mobile Analytics](docs/images/mobile-analytics.png)

---

## 🔁 CI / Quality Guardrails

- Workflow: `.github/workflows/ci.yml`
- Includes:
  - Ruff lint checks
  - Black formatting checks
  - Pytest suite
  - Migration idempotency run
  - Container smoke checks (`/healthz`, `/dashboard`, `/journal`, `/analytics`)
  - Visual smoke guardrail (desktop + mobile screenshots, uploaded as CI artifacts)

## 📡 Monitoring

- Workflow: `.github/workflows/monitoring.yml`
- Scheduled health probe (requires `APP_HEALTH_URL` secret)

---

## 👤 Author

Built by **Kurt McCain** as a trading discipline platform and engineering portfolio project.
