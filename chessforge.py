import subprocess
import sys

# Shorthand for lengthly docker commands that run the CLI inside the container.

cmd = [
    "docker", "compose", "run", "--rm",
    "app",
    "python", "-m", "chessforge.main"
] + sys.argv[1:]

subprocess.run(cmd, check=True)