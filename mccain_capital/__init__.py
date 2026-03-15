"""Package entrypoints for McCain Capital app."""

import hmac
import os
from datetime import timedelta

from flask import abort, redirect, request, session, url_for, render_template_string

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


def create_app():
    """Return configured Flask app with all routes registered."""
    app = core.app
    # Keep modular runtime helpers on the same storage paths as legacy app_core.
    runtime.DB_PATH = core.DB_PATH
    runtime.UPLOAD_DIR = core.UPLOAD_DIR
    runtime.BOOKS_DIR = core.BOOKS_DIR
    runtime.ensure_storage_dirs()

    app.config.from_object(select_config())
    app.config.setdefault("CSRF_ENABLED", True)
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
            content = render_template_string(
                """
                <div class="card pageHero"><div class="toolbar">
                  <div class="pill">🛟 Safe Mode</div>
                  <h2 class="pageTitle">Read-Only Recovery Mode</h2>
                  <div class="pageSub">The app booted with storage/runtime issues. Write operations are blocked until fixed.</div>
                </div></div>
                <div class="card"><div class="toolbar">
                  <div class="pill">Diagnostics</div>
                  <div class="tiny stack8 line16"><b>Error:</b> {{ msg }}</div>
                  <div class="tiny stack8 line16"><b>DB Path:</b> {{ db_path }}</div>
                  <div class="tiny stack8 line16"><b>Upload Dir:</b> {{ upload_dir }}</div>
                  <div class="tiny stack8 line16"><b>Books Dir:</b> {{ books_dir }}</div>
                  <div class="tiny stack8 line16">Next best action: fix mount/path permissions, then restart app.</div>
                </div></div>
                """,
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

        @app.after_request
        def _security_headers(resp):
            resp.headers.setdefault("X-Content-Type-Options", "nosniff")
            resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
            resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
            resp.headers.setdefault(
                "Content-Security-Policy",
                "default-src 'self'; img-src 'self' data: https:; "
                "script-src 'self' 'unsafe-inline' https://s3.tradingview.com https://cdn.plot.ly; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com; connect-src 'self'; frame-ancestors 'self'",
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

        trades_service.ensure_auto_sync_worker_started(app)
        app._auto_sync_worker_started = True
    return app
