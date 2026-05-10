import subprocess
import sys

# Convenience wrapper around Docker CLI execution.
# Runs the Chessforge CLI inside the Docker container.
# Also wraps hosting the MLflow UI on a local web server
#
# Usage:
#   python run.py <command>
#
# Equivalent to:
#   docker compose run --rm app python -m chessforge.cli <command>

CLI_CMD = [
    "docker", "compose", "run", "--rm",
    "app",
    "python","-m", "chessforge.cli"
] + sys.argv[1:]

MLFLOW_UI_CMD = [
    "docker", "compose", "run", "--rm", "--service-ports",
    "app",
    "mlflow", "ui", "--host", "0.0.0.0", "--port", "5000",
    "--backend-store-uri", "sqlite:////app/mlruns/mlflow.db"
]

if sys.argv[1:] == ["mlflow-ui"]:
    subprocess.run(MLFLOW_UI_CMD, check=True)
else:
    subprocess.run(CLI_CMD, check=True)    