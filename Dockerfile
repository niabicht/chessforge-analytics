FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# make sure that we can include from chessforge namespace
COPY src/ ./src/
ENV PYTHONPATH=/app/src 