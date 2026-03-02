"""Authentication service routes."""

from __future__ import annotations

from flask import redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from mccain_capital import auth
from mccain_capital.runtime import set_setting_value
from mccain_capital.services.ui import render_page


def setup_page():
    """First-run auth setup from the web UI."""
    if auth.auth_enabled() and not auth.is_authenticated():
        return redirect(url_for("login_page"))

    err = ""
    msg = ""
    default_user = auth.effective_username() if auth.auth_enabled() else auth.APP_USERNAME

    if request.method == "POST":
        user = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if len(user) < 3:
            err = "Username must be at least 3 characters."
        elif len(password) < 8:
            err = "Password must be at least 8 characters."
        elif password != confirm:
            err = "Passwords do not match."
        else:
            set_setting_value("auth_username", user)
            set_setting_value("auth_password_hash", generate_password_hash(password))
            session["auth_ok"] = True
            session["auth_user"] = user
            session.permanent = True
            msg = "Login credentials saved."
            return redirect(url_for("dashboard"))

    return render_page(
        render_template("setup_login.html", err=err, msg=msg, default_user=default_user),
        active="auth",
        title="McCain Capital · Setup Login",
    )


def login_page():
    if not auth.auth_enabled():
        return redirect(url_for("setup_page"))
    if auth.is_authenticated():
        return redirect(url_for("dashboard"))

    err = ""
    if request.method == "POST":
        user = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        effective_user = auth.effective_username()
        if user == effective_user and check_password_hash(auth.effective_password_hash(), password):
            session["auth_ok"] = True
            session["auth_user"] = effective_user
            session.permanent = True
            next_url = (request.args.get("next") or request.form.get("next") or "").strip()
            if next_url.startswith("/") and not next_url.startswith("//"):
                return redirect(next_url)
            return redirect(url_for("dashboard"))
        err = "Invalid username or password."

    return render_page(
        render_template("login.html", err=err, next_url=request.args.get("next", "")),
        active="auth",
        title="McCain Capital · Login",
    )


def logout_page():
    session.clear()
    return redirect(url_for("login_page"))
