# Daily Intel System

> **An AI-assisted intelligence engine for the EV, Mobility, and Startup ecosystem in India.**

Daily Intel aggregates, deduplicates, and curates high-signal content from scattered sources—career portals, free job APIs, LinkedIn discussions, Twitter/X handles, RSS feeds, and target company ATS boards. Powered by FastAPI, PostgreSQL (Neon / local pgvector), TensorMux (`glm-4-7-flash`), and Resend, it generates noise-free daily digests delivered straight to your inbox on an IST schedule.

---

## 🚀 Quick Start (Local Development)

**Prerequisites:** Python 3.12, Docker + Compose v2

### 1. Start Infrastructure
Boot up the core services (PostgreSQL with `pgvector` and `n8n`):
```bash
docker compose up -d
```

### 2. Setup Python Environment
We strictly use virtual environments for local development.
```powershell
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
python -m alembic upgrade head

# Seed target companies (Required for the Jobs module)
python scripts/seed_from_csv.py
```
> **Note:** Ensure `TENSORMUX_API_KEY` and `RESEND_API_KEY` are populated in your `.env` for LLM-powered summarization and email delivery.

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
| **Jobs Preview** | [http://localhost:8000/admin/jobs/preview](http://localhost:8000/admin/jobs/preview) | Instant preview of rendered HTML for unsent jobs. |
| **News Preview** | [http://localhost:8000/admin/news/preview](http://localhost:8000/admin/news/preview) | Instant preview of rendered HTML for processed news items. |
| **n8n** | [http://localhost:5678](http://localhost:5678) | Workflow automation for RSS and news polling. |

---

## 🧪 Testing & Diagnostics

### Manual Digest Trigger
Trigger digest assembly and dispatch directly to your configured `EMAIL_TO` via Resend:
```powershell
# Send Jobs digest now:
curl.exe -X POST http://localhost:8000/admin/jobs/digest-now

# Send News digest now:
curl.exe -X POST http://localhost:8000/admin/news/digest-now
```

### Simulating an Ingestion Webhook
Test the `POST /ingest` pipeline using a mock payload:
```powershell
curl.exe -s -X POST http://localhost:8000/ingest `
  -H "X-Ingest-Token: dev-token" `
  -H "X-Source-Type: custom_site" `
  -H "Content-Type: application/json" `
  -d '{\"source_identifier\":\"test\",\"items\":[{\"text\":\"Tata Motors launches new commercial EV fleet in Mumbai.\",\"occurred_at\":\"2026-08-31T00:00:00Z\"}]}'
```

### Running the Test Suite
```powershell
# Unit tests (Runs completely offline, no live network required)
pytest tests/ -v -m "not integration"          

# Integration tests (Requires running database)
pytest tests/ -v -m integration               
```

## 🧠 Core Modules Deep Dive

### Module 5: Jobs System (V4 Pipeline)
The Jobs module is the most complex component of Daily Intel. It utilizes a multi-tiered architecture (Architecture V4) to aggregate EV and business roles while aggressively optimizing Apify and Browserbase compute budgets.

#### 1. V4 Scrape Orchestrator (Broad $\to$ Gap $\to$ Target)
Instead of scraping every target company's career page daily, the system uses a smart cascade:
* **Phase 1 (Broad Scrape):** APScheduler triggers broad API/scraper runs (LinkedIn Jobs Apify actor, Adzuna API). These pull thousands of jobs across the ecosystem.
* **Phase 2 (Gap Analyzer):** A deterministic slug matcher (and fallback TensorMux LLM matcher) analyzes the broad jobs and checks which of our 136 `target_companies` were captured.
* **Phase 3 (T1/T2 Cascade):** For target companies *not* covered in Phase 1, the orchestrator triggers targeted scraping. It tries **Tier 1 (Apify Chrome Crawler)**. If Apify returns an empty dataset (e.g., heavily obfuscated SPA), it escalates to **Tier 2 (Browserbase Headless CDP)**. Repeated failures route the company to a **Tier 3 (Watchlist)** circuit breaker.

#### 2. The `JobIn` Contract & Classification
Every job (regardless of source) is normalized into a `JobIn` Pydantic schema. It is then passed through a **Two-Gate Classifier**:
* **Target Sources** (Career Pages, ATS): Gated purely on whether the role is a **Business, Management, or Operations** role using heuristic keyword matrices. (Engineering/Tech roles are filtered out).
* **Broad Sources** (LinkedIn, Adzuna): Gated on **Location** (must be India/Remote) and explicit **EV Domain relevance** (checking descriptions against EV/Mobility taxonomies).

#### 3. Deduplication & Lifecycle
Jobs are persisted using an atomic savepoint pipeline. A SHA-256 `dedup_hash` is calculated over `company + title + location + stripped_url`.
* If a new hash arrives, it is inserted.
* If a duplicate hash arrives, `last_seen_at` is refreshed to today.
* A daily cron job marks any job not seen in the last 21 days as `is_closed = True`.

---

### Module 4: News & Social Intelligence Pipeline
The News module processes high-volume text from various RSS and social channels, using AI to extract high-signal insights.

#### 1. Ingestion Streams
* **n8n Webhook:** Listens for RSS feed updates (global tech, EV specific) and forwards payloads to `POST /ingest/news/n8n-webhook`.
* **Apify Actors:** Specialized actors poll specific Twitter/X handles (e.g., `@teslaclubin`) and LinkedIn hashtags (`#EV`, `#Mobility`).

#### 2. Keyword Pre-Filtering
Before hitting the database or the LLM, raw text is passed through `keyword_filter.py`. This uses regex and entity matching to verify the content relates specifically to the **Indian EV / Startup context**. If 0 keywords hit, the payload is immediately discarded to save database bloat.

#### 3. Text Chunking & LLM Scoring (TensorMux)
Valid items are saved to `raw_items`. At 06:00 IST, `process_unprocessed_news()` is triggered:
* Text is sanitized and chunked to fit within 12,000-character windows.
* It is evaluated by **TensorMux (`glm-4-7-flash`)**. The LLM returns a strictly validated JSON payload containing a synthesized headline, summary, semantic tags, and a `relevance_score` (0.0 to 1.0).
* **Threshold Gate:** Items scoring below 0.40 are marked irrelevant and hidden from the digest.

#### 4. Transaction Safety
Because LLM generation can take 10-30 seconds per item, standard database connections can drop (a known Neon DB idle timeout issue). The news pipeline uses **atomic, short-lived transactions** for each item, guaranteeing stability during long batches.

---

## 🗺 Architecture & Build Sequence

| Phase | Module / Goal | Status |
|:---:|---|:---:|
| **0** | **Scaffolding:** Initial repository setup. | ✅ Built |
| **1** | **Core System (Module 6):** FastAPI, SQLAlchemy, Alembic, and Resend Email Pipeline. | ✅ Built |
| **2** | **Jobs System (Module 5):** V4 Scrape Orchestrator, Gap Analyzer, Apify & Browserbase cascade, Adzuna, LinkedIn Jobs, and ATS integrations. | ✅ Built |
| **3** | **News/RSS (Module 4):** n8n webhook routing, Apify Twitter & LinkedIn news crawlers, TensorMux AI summarizer. | ✅ Built |
| **4** | **Telegram (Module 2):** Telethon ingestion (Planned). | 🚧 Pending |
| **5** | **Personalization (Module 6):** Semantic vector search using `pgvector` (Planned). | 🚧 Pending |
| **6** | **WhatsApp (Module 1):** Baileys integration (Planned). | 🚧 Pending |
| **7** | **LinkedIn (Module 3):** RapidAPI interactions (Planned). | 🚧 Pending |
| **8** | **Production:** Hardening, observability, and cloud deployment. | 🚧 Pending |

