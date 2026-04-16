from typer.testing import CliRunner

from siamquantum_atlas.cli import app


def test_cli_demo_smoke() -> None:
    result = CliRunner().invoke(app, ["demo"])
    assert result.exit_code == 0
    assert "Demo pipeline complete" in result.output


def test_cli_export_alias_smoke() -> None:
    runner = CliRunner()
    runner.invoke(app, ["demo"])
    result = runner.invoke(app, ["export-arena"])
    assert result.exit_code == 0
    assert "Graph export written" in result.output
