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
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# App listens on 5001 by default (matches app.py)
ENV PORT=5001
EXPOSE 5001

# Gunicorn for production
CMD ["gunicorn", "-b", "0.0.0.0:5001", "app:app"]
