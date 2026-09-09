## Context

Market Pulse already creates immutable canonical generations, blocks execution when required
components are stale, retries failed refreshes, and exposes component freshness. The remaining
trust gap is not the absence of safeguards; it is the absence of durable, unified proof. The
current reliability ring is process-local and disappears on restart, health details are spread
across multiple page sections, and source availability cannot be studied over time.

The application is local-only, runs multiple Gunicorn workers in Podman, uses the existing SQLite
database, and must remain useful when a provider is temporarily unavailable. The trader needs a
fast answer to one question: “Can I trust the execution state currently on screen?”

## Goals / Non-Goals

**Goals:**

- Persist a sanitized, bounded history of reliability transitions across workers and restarts.
- Produce one deterministic trust verdict per canonical generation.
- Measure source availability, freshness, latency, failures, fallback use, and recovery.
- Make current trust status obvious while keeping technical evidence one click away.
- Alert once when trust degrades or locks and once when it recovers.
- Preserve the last verified generation and fail closed on incomplete required inputs.

**Non-Goals:**

- Replacing the existing canonical snapshot or refresh coordinator.
- Adding new providers, Redis, cloud monitoring, or public access.
- Persisting raw market-data responses, credentials, or order data.
- Changing strategy, setup, Gamma, or target calculations.
- Treating optional observation data as execution authority.

## Decisions

### 1. Add a bounded reliability event repository in the existing database

Create an additive `market_pulse_reliability_events` table keyed by a stable event id with event
time, ticker, generation id, component, transition, status, reason code, duration, source label,
age, threshold, latency, fallback mode, and sanitized metadata. Use indexed event-time and
component columns. Retain detailed events for 30 days and daily aggregates for 180 days.

This replaces the process-local deque as the durable source while retaining a small in-memory read
cache. SQLite is preferred over log parsing or a second observability service because the app is
single-host and already has additive migrations and repository patterns.

### 2. Derive one trust verdict from required-component completeness

Add a pure evaluator that receives the canonical generation and component diagnostics and returns:

- `Verified`: all required inputs are current, coherent, persisted, and from the expected session.
- `Degraded`: execution remains locked or planning-only, but useful observation data is available.
- `Locked`: a required component is missing, stale, incoherent, or failed persistence.

Every verdict includes reason codes, human text, generation id, evaluated time, next retry, and the
specific required/optional component states. A fresh quote never upgrades stale Gamma, bars, or
persistence.

### 3. Record transitions, not every poll

Generate a durable incident when component or verdict state changes. Repeated identical states
update duration/last-seen information rather than inserting notification noise. Recovery closes the
incident and records recovered-at and total duration. Poll-level metrics remain aggregated counters.

This keeps history useful and bounded while still proving prolonged outages.

### 4. Keep source provenance explicit

Each component reports source, provider timestamp, received time, age, threshold, latency,
fallback/proxy mode, and completeness. SPY volume is labeled as an SPX liquidity proxy; cached
Gamma is labeled cached; observation-only quotes are never described as canonical execution spot.

### 5. Replace repeated status bands with a progressive Trust Center

The top-level surface becomes one compact strip:

`Trust verdict | blockers | last verified | next check | Details`

Opening Details reveals component cards with plain labels, freshness bars, source provenance, and
recent incident transitions. A separate Reliability History page provides daily availability,
incident duration, source failure counts, and recovery timelines. Advanced generation ids and raw
timestamps remain available but do not dominate the default page.

### 6. Reuse the existing application-alert lane

Trust transitions produce sanitized alert events using stable deduplication keys. Notify for first
entry into degraded/locked state and first recovery only. Respect existing mute and acknowledgement
behavior. Reliability alerts never imply a trade action.

### 7. Verify with deterministic faults and restart boundaries

Add tests for stale Gamma, missing bars, provider timeout, malformed source time, failed canonical
write, failed setup ledger write, worker-generation lag, proxy gaps, restart durability, retention,
deduplication, and recovery. Deployed verification covers desktop and narrow Trust Center layouts.

## Risks / Trade-offs

- [Reliability logging creates write pressure] → Record transitions and aggregates, batch updates,
  index narrow fields, and prune under a bounded retention policy.
- [A new verdict conflicts with existing lock state] → The evaluator consumes the existing
  canonical lock as an invariant and tests equivalence before replacing presentation logic.
- [Too much health detail crowds execution] → Show one compact verdict by default and move source
  evidence and history behind progressive disclosure.
- [Alerts become noisy during provider outages] → Deduplicate unchanged incidents and emit only
  transition and recovery events.
- [Database unavailable during an incident] → Keep execution locked, emit local logs, retain the
  in-memory fallback, and never claim durable recording succeeded.
- [Historical availability is mistaken for strategy performance] → Keep reliability history
  separate from setup analytics and label it system quality, not trading quality.

## Migration Plan

1. Add the reliability-event table and repository without changing current UI behavior.
2. Mirror existing process events into durable transitions and compare counts in tests.
3. Add the trust evaluator and response contract while retaining existing lock fields.
4. Replace repeated health presentation with the Trust Center and add history navigation.
5. Enable transition alerts after deduplication and mute behavior are verified.
6. Rebuild, fault-test, restart the container, verify persistence, and inspect both layouts.

Rollback disables durable writes and the new presentation while leaving the additive table dormant.
The existing canonical lock, refresh coordinator, and current health panels remain the fallback.

## Open Questions

None required for implementation. Retention values remain configuration constants so they can be
tuned after real incident volume is observed.
