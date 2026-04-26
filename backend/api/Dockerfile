FROM --platform=linux/amd64 python:3.12
# tesseract-ocr (+ Hebrew language data) is needed by pytesseract for the
# fallback OCR path on scanned-PDF refreshes. poppler-utils provides
# pdftoppm/pdftotext used by pdf2image / PyMuPDF backends.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tesseract-ocr \
        tesseract-ocr-heb \
        poppler-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
WORKDIR /srv
COPY requirements.txt .
COPY requirements-pdf-scraping.txt .
COPY backend/api/requirements.txt requirements-server.txt
RUN pip install -r requirements-server.txt -r requirements.txt -r requirements-pdf-scraping.txt
COPY botnim/ ./botnim
COPY specs/ ./specs
# Seed copy for EFS bootstrap: when /srv/specs/unified/extraction is mounted
# from EFS and that mount is empty (first deploy or after a wipe), the startup
# script copies this seed into the mount so `botnim sync` has content on cold
# start. Kept as a parallel tree so the EFS mount at /srv/specs/... doesn't
# shadow it. After the first successful refresh, this seed is stale; EFS is
# source of truth.
COPY specs/unified/extraction /srv/specs-seed/unified/extraction
COPY setup.py ./
COPY backend/api/server.py .
COPY backend/api/refresh_auth.py .
COPY backend/api/resolve_firebase_user.py .
COPY backend/api/api_server.sh .
COPY firebase-credentials.json ./firebase-credentials.json
RUN pip install -e .
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["./api_server.sh"]
