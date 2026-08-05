# Daily Intel System

> **An AI-assisted intelligence engine for the EV, Mobility, and Startup ecosystem in India.**

Daily Intel aggregates, deduplicates, and curates high-signal content from scattered sources—WhatsApp, Telegram, LinkedIn, RSS feeds, and target company ATS boards. Powered by FastAPI, pgvector, and LLMs, it generates highly personalized, noise-free daily digests delivered straight to your inbox at 07:00 IST.

---

## 🚀 Quick Start (Local Development)

**Prerequisites:** Python 3.12, Docker + Compose v2

### 1. Start Infrastructure
Boot up the core services (PostgreSQL with pgvector, n8n, changedetection.io, and MailHog):
```bash
docker compose up -d
```

### 2. Setup Python Environment
We strictly use virtual environments for local development.
```bash
python -m venv venv

# Windows (PowerShell):
.\venv\Scripts\Activate.ps1

# macOS/Linux (Bash):
# source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configuration & Database Setup
```bash
# Setup environment variables
cp .env.example .env

# Run database migrations
alembic upgrade head

# Seed target companies (Required for the Jobs module)
python scripts/seed_target_companies.py
```
> **Note:** Ensure `GEMINI_API_KEY` is set in your `.env` for LLM-powered summarization and relevance scoring.

### 4. Launch the API
```bash
uvicorn app.main:app --reload --port 8000
```

---

## 🛠 Key Services & URLs

When running locally, you can access the following services:

| Service | Local URL | Description |
|---|---|---|
| **FastAPI Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive API documentation (Swagger UI). |
| **MailHog** | [http://localhost:8025](http://localhost:8025) | Local email preview for testing daily digests. |
| **n8n** | [http://localhost:5678](http://localhost:5678) | Workflow automation for RSS and news polling. |
| **changedetection.io**| [http://localhost:5000](http://localhost:5000) | Website change monitoring for target company career pages. |

---

## 🧪 Testing & Diagnostics

### Manual Digest Trigger
Force the system to assemble and send a digest immediately. Check MailHog to preview it.
```bash
python scripts/manual_send.py
```

### Simulating an Ingestion Webhook
Test the `POST /ingest` pipeline using a mock payload:
```bash
curl -s -X POST http://localhost:8000/ingest \
  -H "X-Ingest-Token: dev-token" \
  -H "X-Source-Type: telegram_channel" \
  -H "Content-Type: application/json" \
  -d @tests/fixtures/sample_ingest.json | python -m json.tool
```

### Running the Test Suite
```bash
# Unit tests (Runs completely offline, no DB required)
pytest tests/ -v -m "not integration"          

# Integration tests (Requires running docker compose infra)
pytest tests/ -v -m integration               
```

---

## 🗺 Architecture & Build Sequence

| Phase | Module / Goal |
|:---:|---|
| **0** | **Scaffolding:** Initial repository setup. |
| **1** | **Core System (Module 6):** FastAPI, SQLAlchemy, Alembic, and the Email Pipeline. |
| **2** | **Jobs System (Module 5):** Apify orchestration, Adzuna, LinkedIn, and ATS integrations. |
| **3** | **News/RSS (Module 4):** n8n webhook routing. |
| **4** | **Telegram (Module 2):** Telethon ingestion. |
| **5** | **Personalization (Module 6):** Semantic vector search using `pgvector`. |
| **6** | **WhatsApp (Module 1):** Baileys integration. |
| **7** | **LinkedIn (Module 3):** RapidAPI interactions. |
| **8** | **Production:** Hardening, observability, and cloud deployment. |

---

## ✨ Recent Updates

**Job Scraping & Filtering:**
- **Concurrent Scaling:** Upgraded the Adzuna scraper with asynchronous pagination, fetching up to 4 pages per keyword concurrently to scale ingestion past standard API limits.
- **Cost-Optimized LinkedIn Scraping:** Migrated LinkedIn Apify scraping to `curious_coder/linkedin-jobs-scraper`. Configured strict constraints (`splitByLocation=False`, `scrapeCompany=False`, max 50 items) to prevent deep company scanning and runaway Apify costs.
- **Hardened Ingestion Pipeline:** Removed legacy bypass logic for LinkedIn and Adzuna. All incoming jobs are now strictly forced through the central EV and business role classifier prior to persistence.

**News Digest & Email Assembly:**
- **Cleaner Batches:** Fixed a major batching bug in the email assembler where job postings were being silently grouped with news queries, causing the generation of blank news emails. 
- **Higher Density:** Increased the `NEWS_BATCH_SIZE` from 20 to 50 articles per email, significantly reducing inbox clutter while maximizing content delivery.
