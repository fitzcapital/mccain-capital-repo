"""WebAuthn / passkey support for passwordless sign-in and device management."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from flask import abort, jsonify, redirect, render_template, request, session, url_for

from mccain_capital import auth
from mccain_capital.runtime import get_setting_value, now_iso, set_setting_value
from mccain_capital.services.ui import render_page

PASSKEYS_SETTING_KEY = "auth_passkeys"
PASSKEY_REGISTER_CHALLENGE_KEY = "_passkey_register_challenge"
PASSKEY_AUTH_CHALLENGE_KEY = "_passkey_auth_challenge"
PASSKEY_AUTH_NEXT_KEY = "_passkey_auth_next"

try:
    from webauthn import (
        generate_authentication_options,
        generate_registration_options,
        verify_authentication_response,
        verify_registration_response,
    )
    from webauthn.helpers import options_to_json
    from webauthn.helpers.base64url_to_bytes import base64url_to_bytes
    from webauthn.helpers.bytes_to_base64url import bytes_to_base64url
    from webauthn.helpers.structs import (
        AuthenticatorSelectionCriteria,
        PublicKeyCredentialDescriptor,
        ResidentKeyRequirement,
        UserVerificationRequirement,
    )

    WEBAUTHN_AVAILABLE = True
except Exception:  # pragma: no cover
    WEBAUTHN_AVAILABLE = False


def _ensure_webauthn_available() -> None:
    if WEBAUTHN_AVAILABLE:
        return
    abort(503, description="Passkeys are unavailable until the WebAuthn dependency is installed.")


def _load_passkeys() -> List[Dict[str, Any]]:
    raw = get_setting_value(PASSKEYS_SETTING_KEY, "[]") or "[]"
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    if not isinstance(parsed, list):
        return out
    for row in parsed:
        if not isinstance(row, dict):
            continue
        credential_id = str(row.get("credential_id") or "").strip()
        public_key = str(row.get("public_key") or "").strip()
        if not credential_id or not public_key:
            continue
        out.append(
            {
                "credential_id": credential_id,
                "public_key": public_key,
                "sign_count": int(row.get("sign_count") or 0),
                "label": str(row.get("label") or "").strip(),
                "added_at": str(row.get("added_at") or ""),
                "last_used_at": str(row.get("last_used_at") or ""),
                "aaguid": str(row.get("aaguid") or ""),
                "fmt": str(row.get("fmt") or ""),
                "credential_device_type": str(row.get("credential_device_type") or ""),
                "credential_backed_up": bool(row.get("credential_backed_up")),
                "transports": list(row.get("transports") or []),
                "created_host": str(row.get("created_host") or ""),
            }
        )
    return out


def _save_passkeys(passkeys: List[Dict[str, Any]]) -> None:
    set_setting_value(PASSKEYS_SETTING_KEY, json.dumps(passkeys, separators=(",", ":")))


def passkeys_available() -> bool:
    return bool(_load_passkeys())


def _request_host() -> str:
    forwarded = str(request.headers.get("X-Forwarded-Host") or "").strip()
    return forwarded or str(request.host or "").strip()


def _request_proto() -> str:
    forwarded = str(request.headers.get("X-Forwarded-Proto") or "").strip().lower()
    if forwarded:
        return forwarded.split(",")[0].strip() or "https"
    return str(request.scheme or "https").strip().lower() or "https"


def _rp_id() -> str:
    return _request_host().split(":", 1)[0].strip("[]").lower()


def _expected_origins() -> List[str]:
    host = _request_host()
    proto = _request_proto()
    origin_header = str(request.headers.get("Origin") or "").strip()
    origins = {
        f"{proto}://{host}".rstrip("/"),
        str(request.host_url or "").rstrip("/"),
    }
    if origin_header:
        origins.add(origin_header.rstrip("/"))
    bare_host = host.split(":", 1)[0].strip("[]").lower()
    if bare_host not in {"127.0.0.1", "localhost", "::1"}:
        origins.add(f"https://{host}".rstrip("/"))
    return sorted(origin for origin in origins if origin)


def _display_passkey_rows() -> List[Dict[str, Any]]:
    rows = []
    for row in _load_passkeys():
        cred = str(row.get("credential_id") or "")
        rows.append(
            {
                **row,
                "device_label": str(row.get("label") or "").strip() or "Unnamed passkey",
                "credential_suffix": cred[-8:] if len(cred) >= 8 else cred,
                "added_label": str(row.get("added_at") or "Unknown"),
                "last_used_label": str(row.get("last_used_at") or "Never used"),
                "host_label": str(row.get("created_host") or _rp_id()),
            }
        )
    return rows


def passkeys_page():
    if not auth.auth_enabled():
        return redirect(url_for("setup_page"))
    _ensure_webauthn_available()
    content = render_template(
        "auth/passkeys.html",
        passkeys=_display_passkey_rows(),
        rp_id=_rp_id(),
        expected_origins=_expected_origins(),
    )
    return render_page(content, active="ops", title="McCain Capital · Passkeys")


def passkeys_register_options():
    _ensure_webauthn_available()
    if not auth.is_authenticated():
        abort(403)

    options = generate_registration_options(
        rp_id=_rp_id(),
        rp_name="McCain Capital",
        user_name=auth.effective_username(),
        user_display_name=auth.effective_username(),
        user_id=auth.effective_username().encode("utf-8"),
        exclude_credentials=[
            PublicKeyCredentialDescriptor(
                id=base64url_to_bytes(str(item.get("credential_id") or "")),
            )
            for item in _load_passkeys()
            if str(item.get("credential_id") or "").strip()
        ],
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.PREFERRED,
            user_verification=UserVerificationRequirement.PREFERRED,
        ),
    )
    session[PASSKEY_REGISTER_CHALLENGE_KEY] = bytes_to_base64url(options.challenge)
    return jsonify({"publicKey": json.loads(options_to_json(options))})


def passkeys_register_verify():
    _ensure_webauthn_available()
    if not auth.is_authenticated():
        abort(403)

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        abort(400, description="Invalid passkey registration payload.")
    challenge = str(session.pop(PASSKEY_REGISTER_CHALLENGE_KEY, "") or "").strip()
    if not challenge:
        abort(400, description="Passkey registration session expired.")

    verified = verify_registration_response(
        credential=payload.get("credential"),
        expected_challenge=base64url_to_bytes(challenge),
        expected_rp_id=_rp_id(),
        expected_origin=_expected_origins(),
        require_user_verification=False,
    )

    credential = payload.get("credential") or {}
    passkeys = [
        row
        for row in _load_passkeys()
        if str(row.get("credential_id") or "") != bytes_to_base64url(verified.credential_id)
    ]
    passkeys.append(
        {
            "credential_id": bytes_to_base64url(verified.credential_id),
            "public_key": bytes_to_base64url(verified.credential_public_key),
            "sign_count": int(verified.sign_count or 0),
            "label": str(payload.get("label") or "").strip()[:80] or f"{_request_host()} device",
            "added_at": now_iso(),
            "last_used_at": "",
            "aaguid": str(verified.aaguid or ""),
            "fmt": str(verified.fmt or ""),
            "credential_device_type": str(verified.credential_device_type or ""),
            "credential_backed_up": bool(verified.credential_backed_up),
            "transports": list((credential.get("response") or {}).get("transports") or []),
            "created_host": _request_host(),
        }
    )
    _save_passkeys(passkeys)
    return jsonify({"ok": True, "count": len(passkeys)})


def passkeys_auth_options():
    _ensure_webauthn_available()
    passkeys = _load_passkeys()
    if not passkeys:
        abort(400, description="No passkeys are registered yet.")

    payload = request.get_json(silent=True) or {}
    next_url = ""
    if isinstance(payload, dict):
        next_url = str(payload.get("next") or "").strip()
    options = generate_authentication_options(
        rp_id=_rp_id(),
        allow_credentials=[
            PublicKeyCredentialDescriptor(
                id=base64url_to_bytes(str(item.get("credential_id") or "")),
            )
            for item in passkeys
            if str(item.get("credential_id") or "").strip()
        ],
        user_verification=UserVerificationRequirement.PREFERRED,
    )
    session[PASSKEY_AUTH_CHALLENGE_KEY] = bytes_to_base64url(options.challenge)
    session[PASSKEY_AUTH_NEXT_KEY] = next_url
    return jsonify({"publicKey": json.loads(options_to_json(options))})


def passkeys_auth_verify():
    _ensure_webauthn_available()
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        abort(400, description="Invalid passkey sign-in payload.")

    challenge = str(session.pop(PASSKEY_AUTH_CHALLENGE_KEY, "") or "").strip()
    next_url = str(payload.get("next") or session.pop(PASSKEY_AUTH_NEXT_KEY, "") or "").strip()
    if not challenge:
        abort(400, description="Passkey sign-in session expired.")

    credential = payload.get("credential")
    if not isinstance(credential, dict):
        abort(400, description="Missing passkey credential.")
    credential_id = str(credential.get("id") or "").strip()
    passkeys = _load_passkeys()
    stored = next(
        (row for row in passkeys if str(row.get("credential_id") or "") == credential_id), None
    )
    if not stored:
        abort(400, description="Unknown passkey.")

    verified = verify_authentication_response(
        credential=credential,
        expected_challenge=base64url_to_bytes(challenge),
        expected_rp_id=_rp_id(),
        expected_origin=_expected_origins(),
        credential_public_key=base64url_to_bytes(str(stored.get("public_key") or "")),
        credential_current_sign_count=int(stored.get("sign_count") or 0),
        require_user_verification=False,
    )
    stored["sign_count"] = int(verified.new_sign_count or stored.get("sign_count") or 0)
    stored["last_used_at"] = now_iso()
    _save_passkeys(passkeys)

    session["auth_ok"] = True
    session["auth_user"] = auth.effective_username()
    session.permanent = True

    if not next_url.startswith("/") or next_url.startswith("//"):
        next_url = url_for("dashboard")
    return jsonify({"ok": True, "redirect": next_url})


def passkeys_delete():
    _ensure_webauthn_available()
    if not auth.is_authenticated():
        abort(403)
    credential_id = str(request.form.get("credential_id") or "").strip()
    if not credential_id:
        abort(400, description="Missing passkey identifier.")
    _save_passkeys(
        [row for row in _load_passkeys() if str(row.get("credential_id") or "") != credential_id]
    )
    return redirect(url_for("passkeys_page"))
