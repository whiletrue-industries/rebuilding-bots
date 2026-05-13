# Default to amd64 to match ECS Fargate. Local Apple Silicon devs override
# via compose (build.args.BASE_PLATFORM=linux/arm64 + platform: linux/arm64)
# so OCR / Playwright run natively at ~5-10× the emulated speed.
ARG BASE_PLATFORM=linux/amd64
FROM --platform=${BASE_PLATFORM} python:3.12
# tesseract-ocr (+ Hebrew language data) is needed by pytesseract for the
# fallback OCR path on scanned-PDF refreshes. poppler-utils provides
# pdftoppm/pdftotext used by pdf2image / PyMuPDF backends.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tesseract-ocr \
        tesseract-ocr-heb \
        poppler-utils \
        # awscli: deploy.sh phase 6.5 (alembic upgrade head) needs to read
        # the cluster master secret on first-run to CREATE EXTENSION vector.
        # Same is true for any future ops scripts that need AWS APIs from
        # inside the running task. Avoids re-introducing the "no aws in
        # container" debt.
        awscli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
WORKDIR /srv
COPY requirements.txt .
COPY requirements-pdf-scraping.txt .
COPY backend/api/requirements.txt requirements-server.txt
RUN pip install -r requirements-server.txt -r requirements.txt -r requirements-pdf-scraping.txt
COPY botnim/ ./botnim
COPY scripts/ ./scripts
# Gold-set lives at deploy/gold-set.json. The committed stub is a
# placeholder (empty array) so CI Docker builds succeed without parlibot
# in the build context. parlibot/deploy.sh phase 3 overwrites this with
# the real gold-set immediately before the production docker build, so
# the image deployed to staging/prod has the actual entries needed by
# the daily scheduled sanity runner (Lambda + EventBridge → /sanity/run
# → botnim.sanity.runner.run_sanity → load_gold_set()).
COPY deploy/gold-set.json /srv/deploy/gold-set.json
COPY specs/ ./specs
# alembic.ini lives at repo root and is required to run schema migrations
# from inside the running task (deploy.sh phase 6.5 invokes
# `alembic -c alembic.ini upgrade head` via `aws ecs run-task` so a fresh
# environment self-bootstraps without a developer-laptop step).
COPY alembic.ini ./
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
COPY backend/api/sanity_auth.py .
COPY backend/api/resolve_firebase_user.py .
COPY backend/api/api_server.sh .
COPY firebase-credentials.json ./firebase-credentials.json
RUN pip install -e .
# Playwright Chromium + system libs for the knesset_sharepoint scraper
# (Reblaze-walled SharePoint pages on main.knesset.gov.il). --with-deps
# pulls libnss3, libxkbcommon0, libatk-bridge2.0-0, etc.
RUN playwright install --with-deps chromium
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["./api_server.sh"]
