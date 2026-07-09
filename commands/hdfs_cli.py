from asyncio import subprocess
import typer
import subprocess
from pathlib import Path
from preprocessing.utils import get_year_from_reviews

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
    typer.echo("Checking folder /steam/silver")

    check = subprocess.run(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-test", "-d", "/steam/silver"])
    if check.returncode != 0:
        typer.echo("Creating folder")
        run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-mkdir", "-p", "/steam/silver/reviews", "/steam/silver/metadata"])
        typer.secho("Creating successfully", fg=typer.colors.GREEN)
    else:
        typer.secho("Thư mục /steam/silver đã tồn tại, bỏ qua bước tạo.", fg=typer.colors.YELLOW)

@app.command("upload_silver")
def upload_silver(source_path: str = "/opt/data/silver/", dest_path: str = "/steam/silver/"):
    """Đưa dữ liệu CSV và games.json từ máy local silver lên HDFS silver"""
    

    # Upload games.json
    # check_game = subprocess.run(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-test", "-d", f"{dest_path}/games.json"])
    # if check_game.returncode == 0:
    #     typer.secho("Games.json already exists, skipping", fg=typer.colors.YELLOW)
    # else:
    #     game_path = f"{source_path}games.json"
    #     game_dest = f"{dest_path}metadata/"
    #     typer.echo(f"Đang upload dữ liệu từ {game_path} lên {game_dest}...")
    #     run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-put", "-f", game_path, game_dest])
    

    # Upload reviews
    year_hdfs = subprocess.run(
        ["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-ls", "/steam/silver/reviews/"],
        capture_output=True,
        text=True,
    )
    if year_hdfs.returncode == 0:
        year_hdfs = year_hdfs.stdout
    else:
        typer.secho(year_hdfs.stderr, fg=typer.colors.RED)
        typer.secho("Vui lòng chạy 'python cli.py hdfs setup' để tạo thư mục.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=1)

    # Get years of /steam/silver/reviews/ in hadoop
    year_hdfs = [line for line in year_hdfs.splitlines() if line.strip() and not line.startswith("Found")]
    year_hdfs = [Path(line.split()[-1]).name for line in year_hdfs]
    
    # Get years of /steam/silver/reviews/ in local
    year_local = get_year_from_reviews("silver")
    year_local = [year.name for year in year_local]
    
    # Get years that are not in hadoop
    unseen_year = [year for year in year_local if year not in year_hdfs]

    for year in unseen_year:
        run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-mkdir", "-p", f"{dest_path}reviews/{year}"])
        print(f"Created folder: {dest_path}reviews/{year}")

        review_path = f"{source_path}reviews/{year}/"
        review_dest = f"{dest_path}reviews/{year}/"
        typer.echo(f"Đang upload dữ liệu từ {review_path} lên {review_dest}...")
        run_cmd(["docker", "exec", CONTAINER_NAME, "hdfs", "dfs", "-put", "-f", review_path, review_dest])

    typer.secho("Upload hoàn tất!", fg=typer.colors.GREEN)