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
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any


STARTER_CONFIG = {
    "inbox": "INBOX",
    "important_folder": "Review/Important",
    "mark_remaining_as_read": True,
    "rules": [
        {
            "name": "Money",
            "folder": "Review/Important/Money",
            "from_contains": ["@chase.com", "@americanexpress.com", "@bankofamerica.com"],
            "subject_contains": ["alert", "transaction", "statement", "payment due"],
        },
        {
            "name": "Security",
            "folder": "Review/Important/Security",
            "from_contains": ["security@", "support@", "noreply@"],
            "subject_contains": ["password", "2fa", "verification", "sign-in"],
        },
        {
            "name": "People",
            "from_contains": ["boss@", "family@", "@important-client.com"],
            "subject_contains": ["urgent", "meeting", "deadline", "call me"],
        },
    ],
}


@dataclass
class Rule:
    name: str
    folder: str | None
    from_contains: list[str]
    subject_contains: list[str]


@dataclass
class Config:
    inbox: str
    important_folder: str
    mark_remaining_as_read: bool
    rules: list[Rule]


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


def _parse_rule(item: Any, idx: int) -> Rule:
    if not isinstance(item, dict):
        raise ValueError(f"Rule #{idx} must be an object.")
    name = str(item.get("name", f"rule-{idx}")).strip()
    folder = str(item.get("folder", "")).strip() or None
    from_contains = _normalize_list(item.get("from_contains"))
    subject_contains = _normalize_list(item.get("subject_contains"))
    if not from_contains and not subject_contains:
        raise ValueError(f"Rule '{name}' must define at least one sender or subject matcher.")
    return Rule(
        name=name,
        folder=folder,
        from_contains=from_contains,
        subject_contains=subject_contains,
    )


def load_config(path: str) -> Config:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Config file '{path}' not found. Run with --write-example-config first."
        ) from exc

    if not isinstance(raw, dict):
        raise ValueError("Config must be a JSON object.")
    rules_raw = raw.get("rules")
    if not isinstance(rules_raw, list) or not rules_raw:
        raise ValueError("Config must contain a non-empty 'rules' array.")

    inbox = str(raw.get("inbox", "INBOX")).strip() or "INBOX"
    important_folder = str(raw.get("important_folder", "Review/Important")).strip()
    if not important_folder:
        raise ValueError("Config field 'important_folder' cannot be empty.")

    rules = [_parse_rule(item, idx) for idx, item in enumerate(rules_raw, start=1)]
    return Config(
        inbox=inbox,
        important_folder=important_folder,
        mark_remaining_as_read=bool(raw.get("mark_remaining_as_read", True)),
        rules=rules,
    )


def rule_matches(rule: Rule, from_value: str, subject_value: str) -> bool:
    from_l = from_value.lower()
    subject_l = subject_value.lower()
    if rule.from_contains and not any(term in from_l for term in rule.from_contains):
        return False
    if rule.subject_contains and not any(term in subject_l for term in rule.subject_contains):
        return False
    return True


def write_example_config(path: str, force: bool) -> Path:
    config_path = Path(path)
    if config_path.exists() and not force:
        raise RuntimeError(
            f"Config file '{config_path}' already exists. Pass --force to overwrite it."
        )
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as f:
        json.dump(STARTER_CONFIG, f, indent=2)
        f.write("\n")
    return config_path


def _mailbox_prefixes(mailbox: str) -> list[str]:
    parts = [part.strip() for part in mailbox.split("/") if part.strip()]
    if not parts:
        return []
    return ["/".join(parts[: idx + 1]) for idx in range(len(parts))]


def _destination_folder(rule: Rule, config: Config) -> str:
    return rule.folder or config.important_folder


def ensure_mailbox(imap: imaplib.IMAP4_SSL, mailbox: str, dry_run: bool) -> None:
    for candidate in _mailbox_prefixes(mailbox):
        status, _ = imap.select(f'"{candidate}"', readonly=True)
        if status == "OK":
            continue
        if dry_run:
            print(f"[dry-run] Would create mailbox: {candidate}")
            continue
        create_status, create_data = imap.create(f'"{candidate}"')
        if create_status != "OK":
            raise RuntimeError(f"Unable to create mailbox '{candidate}': {create_data}")


def fetch_message_headers(imap: imaplib.IMAP4_SSL, msg_id: bytes) -> tuple[str, str]:
    status, data = imap.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])")
    if status != "OK" or not data or not data[0]:
        return "", ""
    message = BytesParser(policy=default).parsebytes(data[0][1])
    return _decode_header_value(message.get("From")), _decode_header_value(message.get("Subject"))


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
    parser.add_argument(
        "--write-example-config",
        action="store_true",
        help="Write a starter JSON config to --config and exit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow --write-example-config to overwrite an existing config file.",
    )
    args = parser.parse_args()

    if args.write_example_config:
        config_path = write_example_config(args.config, force=args.force)
        print(f"Wrote starter config to {config_path}")
        return 0

    imap_host = os.getenv("IMAP_HOST")
    imap_user = os.getenv("IMAP_USER")
    imap_password = os.getenv("IMAP_PASSWORD")
    imap_port = int(os.getenv("IMAP_PORT", "993"))
    if not imap_host or not imap_user or not imap_password:
        raise RuntimeError("Set IMAP_HOST, IMAP_USER, and IMAP_PASSWORD environment variables.")

    config = load_config(args.config)
    ssl_context = ssl.create_default_context()

    with imaplib.IMAP4_SSL(imap_host, imap_port, ssl_context=ssl_context) as imap:
        imap.login(imap_user, imap_password)
        ensure_mailbox(imap, config.important_folder, dry_run=args.dry_run)
        for rule in config.rules:
            ensure_mailbox(imap, _destination_folder(rule, config), dry_run=args.dry_run)

        select_status, select_data = imap.select(config.inbox)
        if select_status != "OK":
            raise RuntimeError(f"Cannot select mailbox '{config.inbox}': {select_data}")

        status, data = imap.search(None, "UNSEEN")
        if status != "OK":
            raise RuntimeError(f"UNSEEN search failed: {data}")
        msg_ids = data[0].split()
        if args.limit > 0:
            msg_ids = msg_ids[: args.limit]

        moved = 0
        moved_by_rule = {rule.name: 0 for rule in config.rules}
        for msg_id in msg_ids:
            from_value, subject_value = fetch_message_headers(imap, msg_id)
            for rule in config.rules:
                if rule_matches(rule, from_value, subject_value):
                    destination = _destination_folder(rule, config)
                    print(
                        f"Matched {msg_id.decode()} via '{rule.name}' -> {destination}"
                        f" | From: {from_value or '(no sender)'}"
                        f" | Subject: {subject_value or '(no subject)'}"
                    )
                    move_message(imap, msg_id, destination, args.dry_run)
                    moved += 1
                    moved_by_rule[rule.name] += 1
                    break

        if not args.dry_run:
            expunge_status, expunge_data = imap.expunge()
            if expunge_status != "OK":
                raise RuntimeError(f"Expunge failed: {expunge_data}")

        marked = 0
        if config.mark_remaining_as_read:
            marked = mark_unseen_as_seen(imap, dry_run=args.dry_run)
        else:
            print("Skipped marking remaining inbox messages as read.")

        print(f"Done. Moved {moved} important emails. Marked {marked} inbox emails as read.")
        for rule_name, count in moved_by_rule.items():
            if count:
                print(f"  - {rule_name}: {count}")
        imap.logout()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
