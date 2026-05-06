import subprocess
import sys

# Convenience wrapper around Docker CLI execution.
# Runs the Chessforge CLI inside the Docker container.
#
# Usage:
#   python chessforge.py <command>
#
# Equivalent to:
#   docker compose run --rm app python -m chessforge.cli <command>

cmd = [
    "docker", "compose", "run", "--rm",
    "app",
    "python","-m", "chessforge.cli"
] + sys.argv[1:]

subprocess.run(cmd, check=True)