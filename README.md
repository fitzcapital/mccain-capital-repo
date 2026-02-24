# McCain Capital 🏛️📈

<p align="center">
  <img src="docs/images/logo.png" alt="McCain Capital Logo" width="180" />
</p>

<p align="center">
  <b>Your personal trading operating system</b><br/>
  Built with Flask + SQLite for journaling, trade review, risk discipline, and execution consistency.
</p>

---

## ✨ What This App Does

- 📅 **Dashboard**: daily/weekly/monthly performance snapshot
- 📋 **Trades**: import broker-style fills, manage trades, bulk actions
- 🧠 **Reviews**: checklist score, setup tags, session analysis
- 🛡️ **Risk Controls**: enforce max-loss behavior and discipline
- 📝 **Journal**: daily context + reflection workflow
- 🧮 **Calculator**: plan risk/reward before entering a trade
- 🎯 **Goals + Payouts**: track progress and payout readiness

---

## 🖼️ Product Showcase

### 📊 Dashboard
![Dashboard](docs/images/dashboard.png)

### 📋 Trades
![Trades](docs/images/trades.png)

### 📝 Journal
![Journal](docs/images/journal.png)

### 🧮 Calculator
![Calculator](docs/images/calculator.png)

### 💸 Payouts
![Payouts](docs/images/payout.png)

### 📱 Mobile Showcase

#### 📊 Dashboard (Mobile)
![Dashboard Mobile](docs/images/mobile-dashboard.png)

#### 📋 Trades (Mobile)
![Trades Mobile](docs/images/mobile-trades.png)

#### 📝 Journal (Mobile)
![Journal Mobile](docs/images/mobile-journal.png)

#### 🧮 Calculator (Mobile)
![Calculator Mobile](docs/images/mobile-calculator.png)

#### 💸 Payouts (Mobile)
![Payouts Mobile](docs/images/mobile-payout.png)

---

## 🧱 Architecture

- `app.py`: compatibility entrypoint
- `mccain_capital/app_core.py`: core app logic + templates
- `mccain_capital/__init__.py`: app factory + hooks
- `mccain_capital/routes/`: route registration
- `mccain_capital/handlers/`: endpoint handlers
- `mccain_capital/config.py`: runtime config profiles
- `services/podman-compose.tailscale.yml`: app + private VPN sidecar

---

## 🗂️ Repo Layout

- `mccain_capital/` -> application code
- `static/` -> logos/icons/favicon + static assets
- `docs/images/` -> README screenshots and branding
- `docs/indicators/` -> indicator reference files
- `services/` -> deployment manifests
- `books/` -> local PDFs for `/books` (not tracked)
- `uploads/` -> runtime import files (not tracked)
- `podman_data/` -> runtime container data (not tracked)

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

---

## 🐳 Quickstart (Podman)

```bash
cd /mccain-capital-repo
podman build -t mccain-capital-app:latest -f Containerfile .
podman rm -f mccain-capital-app 2>/dev/null || true
podman run -d --name mccain-capital-app -p 5001:5001 mccain-capital-app:latest
podman logs -f mccain-capital-app
```

Open: `http://localhost:5001`

---

## 🔐 Private VPN Mode (Tailscale + Podman)

```bash
cd /mccain-capital-repo
export TS_AUTHKEY=tskey-xxxxxxxx
podman compose -f services/podman-compose.tailscale.yml up -d --build
podman compose -f services/podman-compose.tailscale.yml ps
```

---

## 🛠️ Environment Variables

- `SECRET_KEY`
- `DB_PATH`
- `UPLOAD_DIR`
- `BOOKS_DIR`
- `APP_USERNAME`
- `APP_PASSWORD` or `APP_PASSWORD_HASH`
- `SESSION_LIFETIME_MIN`
- `APP_ENV` (`dev` or `prod`)

---

## 🧭 Roadmap

- 📌 Richer review analytics
- 🔄 Schema migrations
- 📈 Weekly auto-reports
- 🔌 Broker integrations

---

## 👤 Author

Built by **Kurt McCain** as a personal trading discipline platform and portfolio project.
