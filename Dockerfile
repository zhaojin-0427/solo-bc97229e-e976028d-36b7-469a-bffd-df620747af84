FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LEAKCHECK_DATA_DIR=/data

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY demo ./demo

# The shared volume is mounted at /data; create it with open permissions so
# the container can run as a non-root user.
RUN mkdir -p /data && useradd -r -u 10001 appuser && chown -R appuser:appuser /data /app
USER appuser

EXPOSE 8000

# Two replicas run this same command; WAL mode allows shared-file access.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
