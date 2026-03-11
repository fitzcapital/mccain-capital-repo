#!/usr/bin/env python3
"""Auto-sort important mail to review folders, then mark inbox mail as read."""

from __future__ import annotations

import argparse
import imaplib
import json
import os
import ssl
from dataclasses import dataclass
from email.header import decode_header
from typing import Any


@dataclass
class Rule:
    name: str
    folder: str
    from_contains: list[str]
    subject_contains: list[str]


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    decoded: list[str] = []
    for text, encoding in parts:
        if isinstance(text, bytes):
            decoded.append(text.decode(encoding or "utf-8", errors="replace"))
        else:
            decoded.append(text)
    return "".join(decoded)


def _normalize_list(values: Any) -> list[str]:
    if not values:
        return []
    if not isinstance(values, list):
        raise ValueError("Rule fields must be arrays of strings.")
    return [str(v).strip().lower() for v in values if str(v).strip()]


def load_rules(path: str) -> list[Rule]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict) or not isinstance(raw.get("rules"), list):
        raise ValueError("Config must be JSON object with a 'rules' array.")

    rules: list[Rule] = []
    for idx, item in enumerate(raw["rules"], start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Rule #{idx} must be an object.")
        name = str(item.get("name", f"rule-{idx}")).strip()
        folder = str(item.get("folder", "")).strip()
        if not folder:
            raise ValueError(f"Rule '{name}' is missing 'folder'.")
        rules.append(
            Rule(
                name=name,
                folder=folder,
                from_contains=_normalize_list(item.get("from_contains")),
                subject_contains=_normalize_list(item.get("subject_contains")),
            )
        )
    return rules


def rule_matches(rule: Rule, from_value: str, subject_value: str) -> bool:
    from_l = from_value.lower()
    subject_l = subject_value.lower()
    from_ok = not rule.from_contains or any(term in from_l for term in rule.from_contains)
    subject_ok = not rule.subject_contains or any(
        term in subject_l for term in rule.subject_contains
    )
    return from_ok and subject_ok


def ensure_mailbox(imap: imaplib.IMAP4_SSL, mailbox: str, dry_run: bool) -> None:
    status, _ = imap.select(f'"{mailbox}"', readonly=True)
    if status == "OK":
        return
    if dry_run:
        print(f"[dry-run] Would create mailbox: {mailbox}")
        return
    create_status, create_data = imap.create(f'"{mailbox}"')
    if create_status != "OK":
        raise RuntimeError(f"Unable to create mailbox '{mailbox}': {create_data}")


def fetch_message_headers(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> tuple[str, str]:
    status, data = imap.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])")
    if status != "OK" or not data or not data[0]:
        return "", ""
    payload = data[0][1].decode("utf-8", errors="replace")
    from_line = ""
    subject_line = ""
    for line in payload.splitlines():
        if line.lower().startswith("from:"):
            from_line = line[5:].strip()
        elif line.lower().startswith("subject:"):
            subject_line = line[8:].strip()
    return _decode_header_value(from_line), _decode_header_value(subject_line)


def move_message(imap: imaplib.IMAP4_SSL, msg_id: bytes, destination: str, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] Would move msg {msg_id.decode()} -> {destination}")
        return
    copy_status, copy_data = imap.copy(msg_id, f'"{destination}"')
    if copy_status != "OK":
        raise RuntimeError(f"Copy failed for msg {msg_id.decode()} to '{destination}': {copy_data}")
    store_status, store_data = imap.store(msg_id, "+FLAGS", r"(\Deleted)")
    if store_status != "OK":
        raise RuntimeError(f"Delete-flag failed for msg {msg_id.decode()}: {store_data}")


def mark_unseen_as_seen(imap: imaplib.IMAP4_SSL, dry_run: bool) -> int:
    status, data = imap.search(None, "UNSEEN")
    if status != "OK":
        raise RuntimeError(f"UNSEEN search failed: {data}")
    ids = data[0].split()
    if not ids:
        return 0
    if dry_run:
        print(f"[dry-run] Would mark {len(ids)} inbox messages as read.")
        return len(ids)
    for msg_id in ids:
        store_status, store_data = imap.store(msg_id, "+FLAGS", r"(\Seen)")
        if store_status != "OK":
            raise RuntimeError(f"Mark-read failed for msg {msg_id.decode()}: {store_data}")
    return len(ids)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Move important inbox emails into review folders and mark inbox as read."
    )
    parser.add_argument(
        "--config",
        default="scripts/email_rules.json",
        help="Path to JSON rules file. Default: scripts/email_rules.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without changing mailbox state.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only inspect first N unread messages (0 = no limit).",
    )
    args = parser.parse_args()

    imap_host = os.getenv("IMAP_HOST")
    imap_user = os.getenv("IMAP_USER")
    imap_password = os.getenv("IMAP_PASSWORD")
    imap_port = int(os.getenv("IMAP_PORT", "993"))
    if not imap_host or not imap_user or not imap_password:
        raise RuntimeError("Set IMAP_HOST, IMAP_USER, and IMAP_PASSWORD environment variables.")

    rules = load_rules(args.config)
    ssl_context = ssl.create_default_context()

    with imaplib.IMAP4_SSL(imap_host, imap_port, ssl_context=ssl_context) as imap:
        imap.login(imap_user, imap_password)
        for rule in rules:
            ensure_mailbox(imap, rule.folder, dry_run=args.dry_run)

        select_status, select_data = imap.select("INBOX")
        if select_status != "OK":
            raise RuntimeError(f"Cannot select INBOX: {select_data}")

        status, data = imap.search(None, "UNSEEN")
        if status != "OK":
            raise RuntimeError(f"UNSEEN search failed: {data}")
        msg_ids = data[0].split()
        if args.limit > 0:
            msg_ids = msg_ids[: args.limit]

        moved = 0
        for msg_id in msg_ids:
            from_value, subject_value = fetch_message_headers(imap, msg_id)
            for rule in rules:
                if rule_matches(rule, from_value, subject_value):
                    move_message(imap, msg_id, rule.folder, args.dry_run)
                    moved += 1
                    break

        if not args.dry_run:
            expunge_status, expunge_data = imap.expunge()
            if expunge_status != "OK":
                raise RuntimeError(f"Expunge failed: {expunge_data}")

        marked = mark_unseen_as_seen(imap, dry_run=args.dry_run)
        print(f"Done. Moved {moved} important emails. Marked {marked} inbox emails as read.")
        imap.logout()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
