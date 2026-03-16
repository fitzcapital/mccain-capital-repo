from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "email_auto_read_and_review.py"
SPEC = importlib.util.spec_from_file_location("email_auto_read_and_review", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_load_config_uses_default_folder_when_rule_has_no_override(tmp_path: Path) -> None:
    config_path = tmp_path / "email_rules.json"
    config_path.write_text(
        json.dumps(
            {
                "inbox": "INBOX",
                "important_folder": "Review/Important",
                "mark_remaining_as_read": True,
                "rules": [
                    {
                        "name": "Security",
                        "from_contains": ["security@"],
                        "subject_contains": ["verification"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    config = MODULE.load_config(str(config_path))

    assert config.inbox == "INBOX"
    assert config.important_folder == "Review/Important"
    assert config.mark_remaining_as_read is True
    assert MODULE._destination_folder(config.rules[0], config) == "Review/Important"


def test_load_config_rejects_rule_without_matchers(tmp_path: Path) -> None:
    config_path = tmp_path / "email_rules.json"
    config_path.write_text(
        json.dumps(
            {
                "rules": [
                    {
                        "name": "Broken Rule",
                        "folder": "Review/Important/Broken",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="at least one sender or subject matcher"):
        MODULE.load_config(str(config_path))


def test_rule_matches_requires_all_configured_groups() -> None:
    rule = MODULE.Rule(
        name="Security",
        folder="Review/Important/Security",
        from_contains=["security@"],
        subject_contains=["verification"],
    )

    assert MODULE.rule_matches(rule, "security@example.com", "Verification code") is True
    assert MODULE.rule_matches(rule, "billing@example.com", "Verification code") is False
    assert MODULE.rule_matches(rule, "security@example.com", "Invoice attached") is False


def test_mailbox_prefixes_expand_nested_folder_path() -> None:
    assert MODULE._mailbox_prefixes("Review/Important/Security") == [
        "Review",
        "Review/Important",
        "Review/Important/Security",
    ]
