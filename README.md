# Daily Intel

**Daily Intel** is an AI-assisted daily intelligence system designed to ingest, filter, summarize, and deliver curated EV/mobility/startup content via a personalized email digest every morning at 07:00 IST.

The system aggregates content from various sources, applies domain-specific relevance filtering (e.g., EV/Mobility business and engineering roles), and leverages LLMs for intelligent summarization and scoring. It's built with personalization in mind using pgvector to learn from user interaction (clicks and skips) over time.

---

## 🚀 Current State: Phase 2 (Jobs Module) is COMPLETE

The project is being built in phases (Modules). **Phase 1 (Core Foundation)** and **Phase 2 (Jobs Module)** are entirely complete and production-ready.

### The Jobs Pipeline (V3 Architecture)
The jobs module acts as a robust, two-tier scraping orchestration system targeting ATS boards, free APIs, and custom career pages.
* **Intelligent Routing (T1 → T2 → T3 Cascade):**
  * **Tier 1 (Apify):** Primary scraper via `playwright:chrome` for SPAs and structured pages.
  * **Tier 2 (Browserbase):** Fallback scraper using CDP via `sync_playwright` for heavily obfuscated or JS-heavy sites that block T1.
  * **Tier 3 (Watchlist):** Temporary suspension for domains failing consecutively, conserving compute limits.
* **ATS Adapters:** Direct integration with Workday, Darwinbox, Greenhouse, Lever, etc.
* **Free APIs:** Nightly polling of Hacker News, Remotive, YC Work at a Startup, and Adzuna.
* **Smart Processing:** 
  * High-accuracy "Matrix Matcher" filtering for specific business/EV roles.
  * Two-layer deduplication (hard URL/title hash + fuzzy `pg_trgm` pass).
  * AI job summarization using OpenRouter/Gemini for concise email formatting.
  * Automated job staleness tracking (auto-closes jobs unseen for 21 days) and email deduplication (`emailed_at` tracking).

### Upcoming Phases
| Phase | Module | Status |
|---|---|---|
| **0 & 1** | Core Foundation (FastAPI, DB, Email Pipeline) | ✅ **Complete** |
| **2** | **Jobs (Apify, Browserbase, ATS, APIs)** | ✅ **Complete** |
| **3** | RSS / Industry News (via n8n) | 🚧 Pending |
| **4** | Telegram (via Telethon) | 🚧 Pending |
| **5** | Personalization (pgvector embeddings) | 🚧 Pending |
| **6** | WhatsApp (Baileys) | 🚧 Pending |
| **7** | LinkedIn Posts (RapidAPI) | 🚧 Pending |
| **8** | Hardening & Observability | 🚧 Pending |

---

## 🛠️ Tech Stack

* **Backend:** Python 3.12, FastAPI, APScheduler
* **Database:** PostgreSQL 16 + pgvector, `pg_trgm` (accessed via async SQLAlchemy & Alembic)
* **Orchestration / Change Detection:** self-hosted n8n, changedetection.io
* **LLMs:** Gemini 2.5 Flash / OpenRouter (for summarization, filtering, and extraction)
* **Email:** Resend (HTML Jinja2 templates)
* **Scraping Infrastructure:** Apify (Tier 1), Browserbase (Tier 2), HTTPX, Playwright

---

## 💻 Quick Start (Local Development)

**Prerequisites:** Python 3.12, Docker + Compose v2.

**1. Start infrastructure** (Postgres, n8n, changedetection, MailHog)
```bash
cd intel-system
docker-compose up -d
```

**2. Setup Python environment**
```bash
python -m venv venv
# Windows:
.\venv\Scripts\Activate.ps1
# macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

**3. Configure Environment**
```bash
cp .env.example .env
# Important: Fill in GEMINI_API_KEY, OPENROUTER_MODEL, APIFY_TOKEN, BROWSERBASE_API_KEY
```

**4. Run database migrations**
```bash
python -m alembic upgrade head
```

**5. Seed Target Companies**
```bash
python scripts/seed_from_csv.py
# Or use the baseline seed: python scripts/seed_target_companies.py
```

**6. Start the API Service**
```bash
uvicorn app.main:app --reload --port 8000
```

### Useful Local URLs
* **FastAPI Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
* **MailHog (Email Preview):** [http://localhost:8025](http://localhost:8025)
* **n8n:** [http://localhost:5678](http://localhost:5678)
* **changedetection.io:** [http://localhost:5000](http://localhost:5000)

---

## 🧪 Admin & Testing

**Trigger a manual job scrape (V3 Orchestrator):**
```bash
curl -X POST http://localhost:8000/admin/jobs/scrape-now
# To run for a specific company: ?company=tatamotors
```

**Preview & Send Digest:**
```bash
# Preview unsent jobs:
curl http://localhost:8000/admin/jobs/preview

# Send digest now:
curl -X POST http://localhost:8000/admin/jobs/digest-now
```

**CLI Testing Tools:**
```bash
# Run unit tests
pytest tests/ -v -m "not integration"

# Run job scraper pipeline diagnostics
python scripts/jobtest.py --target-companies --db-check
```

---

## 📚 Documentation Reference

For detailed internal documentation, architectural decisions, and bug fix reports, see the `docs/` directory:
* `docs/PRD.md` — Core Product Requirements and Build Sequences.
* `docs/architecture-V3.md` — Detailed view of the V3 scraping cascade and intelligence system.
* `docs/dev-checklist.md` — Granular tracking of recent bug fixes, session logs, and DB state for the Job Module.
* `docs/decisions-log.md` — Historical architectural choices and rationale.
