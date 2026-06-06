FROM python:3.12-slim

# Configuración del sistema
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiar código fuente
COPY . .

# Exponer puerto (Railway usa $PORT en runtime)
EXPOSE 8080

# Comando de inicio usando path absoluto de python para gunicorn
# $PORT es inyectado por Railway en runtime
CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 8 --worker-class gthread --timeout 300 --graceful-timeout 30 --keep-alive 75 --access-logfile - --error-logfile -"]
