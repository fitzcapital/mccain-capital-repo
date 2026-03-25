"""Tests for the installable Fitz CLI."""

from __future__ import annotations

import fitz_cli
import pytest


def test_main_dispatches_status_command(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    def _fake_status_main() -> int:
        calls.append("status")
        return 7

    monkeypatch.setattr(fitz_cli, "_run_status_main", _fake_status_main)

    exit_code = fitz_cli.main(["status"])

    assert exit_code == 7
    assert calls == ["status"]


def test_clean_defaults_to_dry_run(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr(fitz_cli, "_find_mole_binary", lambda: "/opt/homebrew/bin/mo")
    calls: list[list[str]] = []
    monkeypatch.setattr(
        fitz_cli,
        "_run_subprocess",
        lambda command: calls.append(list(command)) or 0,
    )

    exit_code = fitz_cli.main(["clean"])

    assert exit_code == 0
    assert calls == [["/opt/homebrew/bin/mo", "clean", "--dry-run"]]
    assert "preview mode enabled by default" in capsys.readouterr().out


def test_clean_apply_runs_real_cleanup(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(fitz_cli, "_find_mole_binary", lambda: "/usr/local/bin/mole")
    calls: list[list[str]] = []
    monkeypatch.setattr(
        fitz_cli,
        "_run_subprocess",
        lambda command: calls.append(list(command)) or 0,
    )

    exit_code = fitz_cli.main(["clean", "--apply", "--debug", "--whitelist"])

    assert exit_code == 0
    assert calls == [["/usr/local/bin/mole", "clean", "--debug", "--whitelist"]]


def test_clean_returns_127_when_mole_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    monkeypatch.setattr(fitz_cli, "_find_mole_binary", lambda: None)

    exit_code = fitz_cli.main(["clean"])

    assert exit_code == 127
    assert "Mole is not installed" in capsys.readouterr().err


def test_main_requires_subcommand():
    with pytest.raises(SystemExit) as excinfo:
        fitz_cli.main([])

    assert excinfo.value.code == 2
