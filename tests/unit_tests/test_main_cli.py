from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

pytest.importorskip("typer")

from typer.testing import CliRunner

from dbt_autofix import main


runner = CliRunner()


@dataclass
class DummyResult:
    refactored: bool
    has_warnings: bool = False

    def print_to_console(self, json_output: bool) -> None:  # pragma: no cover - behaviour tested via exit code
        return None


def _stub_schema_specs(monkeypatch) -> None:
    class _SchemaSpecs:  # pragma: no cover - simple stub container
        pass

    monkeypatch.setattr(main, "SchemaSpecs", lambda *args, **kwargs: _SchemaSpecs())


def test_cli_exit_code_no_changes(monkeypatch, tmp_path: Path) -> None:
    _stub_schema_specs(monkeypatch)

    monkeypatch.setattr(
        main,
        "changeset_all_sql_yml_files",
        lambda *args, **kwargs: ([DummyResult(False)], [DummyResult(False)]),
    )

    result = runner.invoke(main.app, ["deprecations", "--path", str(tmp_path), "--dry-run"])

    assert result.exit_code == 0


def test_cli_exit_code_with_changes(monkeypatch, tmp_path: Path) -> None:
    _stub_schema_specs(monkeypatch)

    monkeypatch.setattr(
        main,
        "changeset_all_sql_yml_files",
        lambda *args, **kwargs: ([DummyResult(True)], []),
    )

    result = runner.invoke(main.app, ["deprecations", "--path", str(tmp_path), "--dry-run"])

    assert result.exit_code == 1


def test_cli_exit_code_with_warnings(monkeypatch, tmp_path: Path) -> None:
    _stub_schema_specs(monkeypatch)

    monkeypatch.setattr(
        main,
        "changeset_all_sql_yml_files",
        lambda *args, **kwargs: ([DummyResult(False, has_warnings=True)], []),
    )

    result = runner.invoke(main.app, ["deprecations", "--path", str(tmp_path), "--dry-run"])

    assert result.exit_code == 1


def test_refactor_yml_returns_status_code(monkeypatch, tmp_path: Path) -> None:
    _stub_schema_specs(monkeypatch)

    monkeypatch.setattr(
        main,
        "changeset_all_sql_yml_files",
        lambda *args, **kwargs: ([DummyResult(True)], []),
    )

    status = main.refactor_yml(path=tmp_path, dry_run=True)

    assert status == 1


def test_refactor_yml_returns_status_for_warnings(monkeypatch, tmp_path: Path) -> None:
    _stub_schema_specs(monkeypatch)

    monkeypatch.setattr(
        main,
        "changeset_all_sql_yml_files",
        lambda *args, **kwargs: ([DummyResult(False, has_warnings=True)], []),
    )

    status = main.refactor_yml(path=tmp_path, dry_run=True)

    assert status == 1


def test_refactor_yml_returns_zero_without_changes(monkeypatch, tmp_path: Path) -> None:
    _stub_schema_specs(monkeypatch)

    monkeypatch.setattr(
        main,
        "changeset_all_sql_yml_files",
        lambda *args, **kwargs: ([DummyResult(False)], []),
    )

    status = main.refactor_yml(path=tmp_path, dry_run=True)

    assert status == 0
