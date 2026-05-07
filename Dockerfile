FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install dependencies without caching unnecessary pip stuff (smaller image size)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and ensure Python can resolve internal package imports (chessforge.*)
COPY src/ ./src/
ENV PYTHONPATH=/app/src

# Copy tests folder and files
COPY tests/ ./tests/
COPY conftest.py .
COPY pytest.ini .

# Default runtime is handled via docker-compose or python wrapper