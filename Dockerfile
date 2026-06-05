FROM python:3.12-slim

WORKDIR /app

# Sin gcc — boto3 trae wheels precompilados, ahorra ~200MB y tiempo de build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# 1 worker + muchos threads = sesiones compartidas en memoria + concurrencia
# timeout alto para uploads grandes
CMD gunicorn --bind 0.0.0.0:${PORT:-5000} \
    --workers 1 \
    --threads 16 \
    --worker-class gthread \
    --timeout 300 \
    --graceful-timeout 30 \
    --keep-alive 75 \
    --access-logfile - \
    --error-logfile - \
    app:app
