FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY chessforge_analytics ./chessforge_analytics
CMD ["python", "-m", "chessforge_analytics.main"]