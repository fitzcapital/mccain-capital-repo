"""Package entrypoints for McCain Capital app."""

import os
from datetime import timedelta

from flask import redirect, request, url_for

from mccain_capital.config import select_config
from mccain_capital import app_core as core
from mccain_capital import runtime
from mccain_capital.routes import register_all_routes


def create_app():
    """Return configured Flask app with all routes registered."""
    app = core.app
    # Keep modular runtime helpers on the same storage paths as legacy app_core.
    runtime.DB_PATH = core.DB_PATH
    runtime.UPLOAD_DIR = core.UPLOAD_DIR
    runtime.BOOKS_DIR = core.BOOKS_DIR
    os.makedirs(os.path.dirname(runtime.DB_PATH) or ".", exist_ok=True)
    os.makedirs(runtime.UPLOAD_DIR, exist_ok=True)
    os.makedirs(runtime.BOOKS_DIR, exist_ok=True)

    app.config.from_object(select_config())
    app.secret_key = app.config["SECRET_KEY"]
    app.permanent_session_lifetime = timedelta(
        minutes=app.config["PERMANENT_SESSION_LIFETIME_MINUTES"]
    )

    if not getattr(app, "_routes_registered", False):
        register_all_routes(app)
        app._routes_registered = True

    if not getattr(app, "_security_hooks_registered", False):

        @app.before_request
        def _auth_gate():
            if not core.auth_enabled():
                return None
            allow = {"login_page", "logout_page", "healthz", "favicon", "static"}
            if request.endpoint in allow:
                return None
            if core.is_authenticated():
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
                "script-src 'self' 'unsafe-inline' https://s3.tradingview.com; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com; connect-src 'self'; frame-ancestors 'self'",
            )
            return resp

        app._security_hooks_registered = True
    core.init_db()
    return app
