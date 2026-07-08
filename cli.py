import typer
from commands import hdfs_cli, pipeline

# Tạo app chính
app = typer.Typer()

# Add các sub-command vào app chính
# app.add_typer(hdfs_cli.app, name="hdfs")
app.add_typer(pipeline.app, name="pipeline")
app.add_typer(hdfs_cli.app, name="hdfs")
if __name__ == "__main__":
    app()