# Containerfile (Dockerfile compatible)
FROM python:3.11-slim

# System deps:
# - poppler-utils: required by pdf2image
# - tesseract-ocr: required by pytesseract
# - libgl1/libglib2.0-0: common image deps (safe add)
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    tesseract-ocr \
    libgl1 \
    libglib2.0-0 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libxshmfence1 \
    libgtk-3-0 \
    fonts-liberation \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install chromium

COPY . .

# App listens on 5001 by default (matches app.py)
ENV PORT=5001
ENV AUTO_SYNC_PASSWORD_FALLBACK=1
EXPOSE 5001

# Gunicorn for production.
# Live broker sync can exceed the default 30s request timeout, so raise the timeout
# and keep an extra worker available while one request is busy running Playwright.
CMD ["gunicorn", "--workers", "2", "--timeout", "180", "--graceful-timeout", "30", "-b", "0.0.0.0:5001", "mccain_capital.wsgi:app"]
