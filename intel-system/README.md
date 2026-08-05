# Daily Intel System

One daily email at 07:00 IST: curated, summarized, ranked content from WhatsApp, Telegram, LinkedIn, RSS news, and job feeds. Personalized via pgvector over time.

## Quick start (local)

**Prerequisites:** Python 3.12, Docker + Compose v2

```bash
# 1. Start infra (Postgres, n8n, changedetection, MailHog)
docker compose up -d

# 2. Set up Python env (always use venv for local dev)
python -m venv venv
# Activate — Windows PowerShell:
.\venv\Scripts\Activate.ps1
# Activate — bash/macOS/Linux:
# source venv/bin/activate

pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — at minimum set GEMINI_API_KEY if you want LLM features

# 4. Run migrations
alembic upgrade head

# 5. Seed target companies (Phase 2)
python scripts/seed_target_companies.py

# 6. Start the API
uvicorn app.main:app --reload --port 8000
```

## Key URLs (local)

| Service | URL |
|---|---|
| FastAPI docs | http://localhost:8000/docs |
| MailHog (email preview) | http://localhost:8025 |
| n8n | http://localhost:5678 |
| changedetection.io | http://localhost:5000 |

## Manual digest send

```bash
python scripts/manual_send.py
```

Check MailHog at http://localhost:8025 to see the rendered email.

## Test a raw ingest

```bash
curl -s -X POST http://localhost:8000/ingest \
  -H "X-Ingest-Token: dev-token" \
  -H "X-Source-Type: telegram_channel" \
  -H "Content-Type: application/json" \
  -d @tests/fixtures/sample_ingest.json | python -m json.tool
```

## Running tests

```bash
pytest tests/ -v -m "not integration"          # unit tests (no DB)
pytest tests/ -v -m integration               # needs docker compose up
```

## Build sequence

| Phase | What |
|---|---|
| 0 | Scaffold (this) |
| 1 | Module 6 core — FastAPI + DB + email pipeline |
| 2 | Module 5 — Jobs (Apify + free APIs + ATS) |
| 3 | Module 4 — RSS/news via n8n |
| 4 | Module 2 — Telegram (Telethon) |
| 5 | Module 6 personalization — pgvector |
| 6 | Module 1 — WhatsApp (Baileys) |
| 7 | Module 3 — LinkedIn (RapidAPI) |
| 8 | Hardening + observability + prod deploy |

## Recent Updates

**Job Scraping & Filtering:**
- Upgraded the Adzuna scraper with concurrent pagination (fetching up to 4 pages per keyword simultaneously) to scale ingestion past the 50 results-per-page limit.
- Migrated LinkedIn Apify scraping to the `curious_coder/linkedin-jobs-scraper` actor. Configured with strict caps (`splitByLocation=False`, `scrapeCompany=False`, max 50 items) to prevent deep company scraping and runaway costs.
- Hardened the ingestion pipeline: Removed legacy bypass logic for LinkedIn/Adzuna so all incoming jobs must pass the central EV and business role classifier before persisting.

**News Digest & Email Assembly:**
- Fixed a major batching bug in `assembler.py` where job postings were being silently merged into the news digest queries, causing empty HTML blocks and blank news batches.
- Increased the News batch size (`NEWS_BATCH_SIZE`) from 20 to 50 articles per email to reduce inbox clutter.
- Resolved minor syntax (`Any` NameError) and logging issues in the email generation code.
