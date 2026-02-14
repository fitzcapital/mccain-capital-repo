# 📈 Trading Journal & Dashboard (Flask + SQLite)

A single-file Flask web app to **log trades**, **journal your day**, and **track performance + payouts** — backed by a simple **SQLite database**. ✅

---

## ✨ Features

### 🧾 Trades (core)
- ✅ Add/edit/delete trades
- 🗓️ View trades by **day** (`/trades?d=YYYY-MM-DD`)
- 📊 Auto stats per day (wins/losses, win rate, P/L totals, etc.)
- 🧮 Running **overall balance** + “as-of date” balance (used across Trades + Dashboard)

### ☑️ Bulk actions on Trades
- ☑️ **Select multiple trades**
- 🗑️ **Delete Selected**
- 📋 **Copy Selected to another date** (multi-copy to a different day of the week)

### 📥 Import / Paste trades
- 📋 Paste trades into the app (supports multiple formats via auto-detect)
- 🏦 Paste “broker-style” statements (supported format depends on what you paste)
- 📄 Upload statement files (HTML/PDF) to extract data (if enabled in your build)

### 📒 Journal
- 📝 Create daily journal entries
- ✏️ Edit entries
- 🗑️ Delete entries

### 🧠 Strategies
- ➕ Add strategies
- ✏️ Edit strategies
- 🗑️ Delete strategies
- 📋 Use strategy names while logging trades (so you can track what works)

### 💸 Payout tracking & goals
- 🧱 Protect buffer tracking (so you don’t give payouts back)
- 🎯 Account size + goal metrics
- 📆 Summary views for recent performance windows

### 📚 Extras
- 📖 Books page (reading list / resources)
- 🔗 Links page (quick access to tools/resources)
- 📦 Export your data as JSON (`/export.json`) for backups or migrations

---

## 🛠 Tech stack
- **Python** 🐍
- **Flask** 🌶️
- **SQLite** 🗃️
- Single-file app (`app_patched.py`) with embedded HTML templates

---

## 🚀 Quickstart

### 1) Install dependencies
```bash
python -m venv .venv
source .venv/bin/activate
pip install flask
