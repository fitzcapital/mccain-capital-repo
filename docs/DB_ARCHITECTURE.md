# McCain Capital Database Architecture

This document explains how the SQLite database is structured, how tables relate, and how data moves through the app.

## 1) Database Basics

- Engine: SQLite
- Default file: `journal.db`
- Migration source: `mccain_capital/migrations/__init__.py`
- Migration history table: `schema_migrations`

Current migration versions:
- `0001_baseline`
- `0002_journal_phase2`

## 2) ER Diagram

```mermaid
erDiagram
    SETTINGS {
        text key PK
        text value
    }

    TRADES {
        int id PK
        text trade_date
        text entry_time
        text exit_time
        text ticker
        text opt_type
        real strike
        real entry_price
        real exit_price
        int contracts
        real total_spent
        real comm
        real gross_pl
        real net_pl
        real result_pct
        real balance
        text raw_line
        text created_at
    }

    TRADE_REVIEWS {
        int id PK
        int trade_id UK
        text setup_tag
        text session_tag
        int checklist_score
        text rule_break_tags
        text review_note
        text created_at
        text updated_at
    }

    ENTRIES {
        int id PK
        text entry_date
        text market
        text setup
        text grade
        real pnl
        text mood
        text notes
        text entry_type
        text template_payload
        text created_at
        text updated_at
    }

    ENTRY_TRADE_LINKS {
        int entry_id PK
        int trade_id PK
        text created_at
    }

    RISK_CONTROLS {
        int id PK
        real daily_max_loss
        int enforce_lockout
        text updated_at
    }

    STRATEGIES {
        int id PK
        text title
        text body
        text created_at
        text updated_at
    }

    DAILY_GOALS {
        int id PK
        text track_date UK
        real debt_paid
        text debt_note
        int upwork_proposals
        int upwork_interviews
        real upwork_hours
        real upwork_earnings
        real other_income
        text notes
        text created_at
        text updated_at
    }

    SCHEMA_MIGRATIONS {
        text id PK
        text applied_at
    }

    TRADES ||--o| TRADE_REVIEWS : "1-to-1 by trade_id"
    ENTRIES ||--o{ ENTRY_TRADE_LINKS : "1-to-many"
    TRADES ||--o{ ENTRY_TRADE_LINKS : "1-to-many"
```

## 3) Table-by-Table Purpose

- `settings`
  - Global key/value config (app-level tunables).

- `trades`
  - Canonical ledger for imported/manual trades.
  - Analytics and dashboard P/L metrics read from here.

- `trade_reviews`
  - Review metadata attached to a trade (`trade_id` is unique).
  - Powers setup/session analytics and rule-break summaries.

- `entries`
  - Journal entries (pre-market plan, debrief, post-market review).
  - Holds free-text notes and structured metadata.

- `entry_trade_links`
  - Bridge table for many-to-many linking:
  - One journal entry can reference many trades.
  - One trade can be referenced by multiple entries.

- `risk_controls`
  - Singleton policy row used by lockout guardrail logic.

- `strategies`
  - Strategy/playbook records.

- `daily_goals`
  - Daily goals/progress tracking.

- `schema_migrations`
  - Internal migration bookkeeping table.

## 4) High-Level Data Flows

1. Statement import flow
- Raw statement/broker lines are parsed into fills.
- Fills are paired into completed round-trip trades.
- Trades are inserted into `trades`.
- Auto-review metadata is inserted into `trade_reviews`.
- Duplicate protection prevents reinserting identical imports.

2. Journal flow
- User creates/edits row in `entries`.
- Linked trades are persisted to `entry_trade_links`.
- Weekly review joins `entries` + `entry_trade_links` + `trade_reviews`.

3. Analytics flow
- Reads primarily from `trades` + `trade_reviews`.
- Aggregates by setup/session/hour and computes expectancy, drawdown, correlation, trends.

4. Risk-control flow
- Reads `risk_controls` + day totals from `trades`.
- Determines whether day lockout should block new trade imports/entries.

## 5) Why Migrations Matter Here

Before migrations, schema drift was handled in runtime code (`CREATE/ALTER` checks in multiple places).  
Now schema evolution is centralized and versioned:
- deterministic startup behavior
- safer deploys
- clear upgrade path for future schema changes

## 6) Useful Dev Commands

Run migrations manually:

```bash
python migrate.py
```

Check applied migration versions:

```bash
sqlite3 journal.db "SELECT id, applied_at FROM schema_migrations ORDER BY id;"
```

Quick table list:

```bash
sqlite3 journal.db ".tables"
```

