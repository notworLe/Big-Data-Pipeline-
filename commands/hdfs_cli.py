import typer
import subprocess

app = typer.Typer(help="Command line for HDFS docker")
CONTAINER_NAME = "namenode"

def run_cmd(command: list):
    """Hàm helper để chạy lệnh shell và in lỗi nếu có"""
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        typer.secho(f"Lỗi: {result.stderr}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    return result.stdout

@app.command("setup")
def setup_dfs():
    """Tạo các thư mục cần thiết trên HDFS (Idempotent)"""
    typer.echo("Checking folder /steam/reviews")

    check = subprocess.run(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-test", "-d", "/steam/reviews"])
    if check.returncode != 0:
        typer.echo("Creating folder")
        run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-mkdir", "-p", "/steam/reviews/merged", "/steam/metadata"])
        typer.secho("Creating successfully", fg=typer.colors.GREEN)
    else:
        typer.secho("Thư mục /steam/reviews đã tồn tại, bỏ qua bước tạo.", fg=typer.colors.YELLOW)

@app.command("upload")
def upload_data(source_path: str = "/opt/data/", dest_path: str = "/steam/"):
    """Đưa dữ liệu CSV và games.json từ máy local lên HDFS"""

    game_path = f"{source_path}metadata/games.json"
    game_dest = f"{dest_path}metadata/"
    typer.echo(f"Đang upload dữ liệu từ {game_path} lên {game_dest}...")
    run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-put", "-f", game_path, game_dest])

    review_path = f"{source_path}reviews/merged/"
    review_dest = f"{dest_path}reviews/merged/"
    typer.echo(f"Đang upload dữ liệu từ {review_path} lên {review_dest}...")
    run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-put", "-f", review_path, review_dest])

    typer.secho("Upload hoàn tất!", fg=typer.colors.GREEN)