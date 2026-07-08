import typer
from commands import hdfs_cli, merge_data

app = typer.Typer(help="Run full pipeline")

@app.command("run")
def run_ful_pipeline():
    """Chạy tuần tự Preprocess -> HDFS Setup -> HDFS Upload"""
    typer.secho("=== BẮT ĐẦU PIPELINE ===", fg=typer.colors.CYAN, bold=True)

    merge_data.merge()
    hdfs_cli.setup_dfs()
    hdfs_cli.upload_data()

    typer.secho("=== PIPELINE HOÀN TẤT ===", fg=typer.colors.CYAN, bold=True)