"""Package entrypoints for McCain Capital app."""

import hmac
import os
import secrets
import time
from datetime import timedelta

from flask import abort, g, redirect, render_template, request, session, url_for

from mccain_capital import auth
from mccain_capital.config import select_config
from mccain_capital import app_core as core
from mccain_capital import runtime
from mccain_capital.routes import register_all_routes

_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def _csrf_enabled(app) -> bool:
    if app.config.get("TESTING") and not app.config.get("CSRF_ENABLED", False):
        return False
    return bool(app.config.get("CSRF_ENABLED", True))


def _validate_csrf() -> bool:
    sent = str(request.headers.get("X-CSRF-Token") or request.form.get("csrf_token") or "").strip()
    expected = str(session.get("_csrf_token") or "").strip()
    return bool(sent and expected and hmac.compare_digest(sent, expected))


def _ensure_csrf_token() -> str:
    token = str(session.get("_csrf_token") or "").strip()
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def create_app():
    """Return configured Flask app with all routes registered."""
    app = core.app
    # Keep modular runtime helpers on the same storage paths as legacy app_core.
    runtime.DB_PATH = core.DB_PATH
    runtime.UPLOAD_DIR = core.UPLOAD_DIR
    runtime.BOOKS_DIR = core.BOOKS_DIR
    runtime.clear_settings_cache()
    runtime.ensure_storage_dirs()

    app.config.from_object(select_config())
    app.config.setdefault("CSRF_ENABLED", True)
    app.config.setdefault("REQUEST_PROFILING_ENABLED", True)
    app.config.setdefault(
        "REQUEST_SLOW_MS",
        max(1, int(os.environ.get("REQUEST_SLOW_MS", "400") or 400)),
    )
    app.config.setdefault(
        "REQUEST_PROFILING_LOG_ALL",
        str(os.environ.get("REQUEST_PROFILING_LOG_ALL", "")).strip().lower()
        in {"1", "true", "yes", "on"},
    )
    env = os.environ.get("APP_ENV", "dev").lower().strip()
    secret_key = runtime.load_or_create_secret_key()
    if env in {"prod", "production"} and not secret_key:
        raise RuntimeError("SECRET_KEY must be set when APP_ENV=prod.")
    app.config["SECRET_KEY"] = secret_key
    app.secret_key = secret_key
    app.permanent_session_lifetime = timedelta(
        minutes=app.config["PERMANENT_SESSION_LIFETIME_MINUTES"]
    )

    if not getattr(app, "_routes_registered", False):
        register_all_routes(app)
        app._routes_registered = True

    if not getattr(app, "_safe_mode_route_registered", False):

        @app.get("/safe-mode")
        def safe_mode_page():
            msg = str(app.config.get("SAFE_MODE_ERROR") or "Unknown startup fault")
            content = render_template(
                "core/safe_mode.html",
                msg=msg,
                db_path=runtime.DB_PATH,
                upload_dir=runtime.UPLOAD_DIR,
                books_dir=runtime.BOOKS_DIR,
            )
            return core.render_page(
                content, active="dashboard", title="McCain Capital 🏛️ · Safe Mode"
            )

        app._safe_mode_route_registered = True

    if not getattr(app, "_security_hooks_registered", False):

        @app.before_request
        def _auth_gate():
            if app.config.get("REQUEST_PROFILING_ENABLED", True):
                g._request_started_at = time.perf_counter()
                runtime.reset_request_metrics()
            if request.method.upper() not in _UNSAFE_METHODS:
                _ensure_csrf_token()
            if app.config.get("SAFE_MODE"):
                allow_safe = {"safe_mode_page", "healthz", "favicon", "static"}
                if request.endpoint not in allow_safe:
                    return redirect(url_for("safe_mode_page"))
            if (
                _csrf_enabled(app)
                and request.method.upper() in _UNSAFE_METHODS
                and request.endpoint not in {"static", "healthz"}
                and not _validate_csrf()
            ):
                abort(400, description="CSRF token missing or invalid.")
            if not auth.auth_enabled():
                return None
            allow = {
                "login_page",
                "logout_page",
                "passkeys_auth_options",
                "passkeys_auth_verify",
                "healthz",
                "favicon",
                "static",
                "vanquish_lock_state",
            }
            if request.endpoint in allow:
                return None
            if auth.is_authenticated():
                return None
            nxt = request.full_path if request.query_string else request.path
            return redirect(url_for("login_page", next=nxt))

        @app.context_processor
        def _csrf_context():
            return {"csrf_token": _ensure_csrf_token()}

        @app.after_request
        def _security_headers(resp):
            if app.config.get("REQUEST_PROFILING_ENABLED", True):
                started_at = getattr(g, "_request_started_at", None)
                if isinstance(started_at, (int, float)):
                    total_ms = max(0.0, (time.perf_counter() - started_at) * 1000.0)
                    metrics = runtime.get_request_metrics_snapshot()
                    sql_ms = float(metrics.get("sql_total_ms") or 0.0)
                    sql_queries = int(metrics.get("sql_query_count") or 0.0)
                    resp.headers["X-Request-Duration-Ms"] = f"{total_ms:.2f}"
                    resp.headers["X-SQLite-Duration-Ms"] = f"{sql_ms:.2f}"
                    resp.headers["X-SQLite-Query-Count"] = str(sql_queries)
                    resp.headers["Server-Timing"] = (
                        f"app;dur={total_ms:.2f}, sqlite;dur={sql_ms:.2f};desc=\"{sql_queries} queries\""
                    )
                    slow_ms = float(app.config.get("REQUEST_SLOW_MS", 400) or 400)
                    if app.config.get("REQUEST_PROFILING_LOG_ALL") or total_ms >= slow_ms:
                        app.logger.info(
                            "request_profile method=%s path=%s status=%s total_ms=%.2f sql_ms=%.2f sql_queries=%s",
                            request.method,
                            request.path,
                            resp.status_code,
                            total_ms,
                            sql_ms,
                            sql_queries,
                        )
            resp.headers.setdefault("X-Content-Type-Options", "nosniff")
            resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
            resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
            resp.headers.setdefault(
                "Content-Security-Policy",
                "default-src 'self'; img-src 'self' data: https:; "
                "script-src 'self' 'unsafe-inline' https://s3.tradingview.com https://cdn.plot.ly https://platform.twitter.com https://cdn.syndication.twimg.com https://abs.twimg.com https://elfsightcdn.com https://*.elfsight.com; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com; connect-src 'self' https://platform.twitter.com https://syndication.twitter.com https://cdn.syndication.twimg.com https://cdn.syndication.twitter.com https://abs.twimg.com https://elfsightcdn.com https://*.elfsight.com; "
                "frame-src 'self' https://s.tradingview.com https://www.tradingview.com https://platform.twitter.com https://syndication.twitter.com https://twitter.com https://x.com https://cdn.syndication.twimg.com https://elfsightcdn.com https://*.elfsight.com; "
                "frame-ancestors 'self'",
            )
            return resp

        app._security_hooks_registered = True
    try:
        core.init_db()
        app.config["SAFE_MODE"] = False
        app.config["SAFE_MODE_ERROR"] = ""
    except Exception as e:
        app.config["SAFE_MODE"] = True
        app.config["SAFE_MODE_ERROR"] = str(e)
    if app.config.get("SAFE_MODE"):
        return app
    if not getattr(app, "_auto_sync_worker_started", False):
        from mccain_capital.services import trades_sync as trades_service

        trades_service.prepare_sync_runtime_state()
        trades_service.ensure_auto_sync_worker_started(app)
        app._auto_sync_worker_started = True
    return app
