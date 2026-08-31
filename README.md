# ⚡ Daily Intel

> **AI-assisted Autonomous Intelligence & Job Aggregation Engine for the Indian EV, Mobility, and Startup Ecosystem.**

**Daily Intel** is an end-to-end intelligence aggregation, deduplication, AI-scoring, and digest delivery platform. It monitors hundreds of target career portals, free job APIs, global & domestic RSS feeds, Twitter/X handles, LinkedIn discussions, and industry sites. It applies high-precision domain classification and heuristic filtering, summarizes content via LLMs, and dispatches automated, cleanly formatted email digests on an India Standard Time (IST) schedule.

---

## 📦 Modules Overview (Scope & Implementation Status)

The system is rigorously partitioned into **eight distinct modules / phases** as defined in the Product Requirements Document (`docs/PRD.md`). This guarantees decoupled ingestion sources that funnel into a unified processing and delivery plane.

| Module | Name / Focus | Status | Implementation Details |
|:---:|---|:---:|---|
| **0 & 1** | **Core System (Foundation & Delivery)** | ✅ **Built** | FastAPI backend, async SQLAlchemy + Alembic migrations, Neon PostgreSQL DB. Robust Resend API email pipeline. APScheduler cron triggers configured for Asia/Kolkata timezone. |
| **5** | **Jobs System (V4 Orchestration)** | ✅ **Built** | Fully operational cascade: Broad scrapers (LinkedIn Jobs Apify + Adzuna) $\to$ Gap Analyzer (Deterministic + LLM matching) $\to$ Targeted career page cascade (T1 Apify $\to$ T2 Browserbase $\to$ T3 Watchlist). Includes strict EV / business role classification and robust `pg_trgm` fuzzy deduplication. Stale jobs auto-close after 21 days. |
| **4** | **News & RSS Intelligence** | ✅ **Built** | Operational news ingestion via n8n (RSS webhook), Apify Twitter poller, and Apify LinkedIn News/Community pollers. Includes Indian EV keyword & entity pre-filtering, 12,000-char window chunking, and **TensorMux (`glm-4-7-flash`)** LLM summarization and relevance scoring ($\ge 0.40$). |
| **2** | **Telegram Integration** | 🚧 *Pending* | Planned Telethon user-API client for tracking specific Telegram groups (read-only). Will use persistent sessions and integrate with the `/ingest` webhook contract. |
| **6** | **Personalization & pgvector Memory** | 🚧 *Pending* | Semantic vector search using `pgvector`. Will generate embeddings for processed items, inject top-3 historical context into the LLM summarizer prompt, and utilize email tracking pixels for preference re-ranking. |
| **1** | **WhatsApp Group Monitoring** | 🚧 *Pending* | Planned Node.js Baileys microservice running on a dummy account. Strict read-only enforcement to safely extract group messages and forward to FastAPI `/ingest`. |
| **3** | **LinkedIn Creator Posts** | 🚧 *Pending* | RapidAPI integration for extracting deep posts from specific EV executives and creators. |
| **8** | **Production Hardening** | 🚧 *Pending* | Final deployment to Hetzner CX32 VPS (infra) + Koyeb (FastAPI). Sentry observability, automated `pg_dump` backups, and Resend domain validation (SPF/DKIM/DMARC). |

---

## 🧠 V4 Architecture Deep Dive

Daily Intel is currently running **Architecture V4**. Evolved from its original hybrid design, it is a highly resilient pipeline centered around **FastAPI**, **APScheduler**, **PostgreSQL (Neon)**, and **TensorMux** for robust LLM processing. 

### Key Architectural Tenets of V4:
1. **LLM Backend Shift**: Migrated from OpenRouter to TensorMux (using `glm-4-7-flash` via `qwen3-6-35b-a3b` endpoints) to handle large context extraction and prevent timeouts caused by reasoning tags (`</think>`).
2. **Database Reliability**: Addressed connection idle timeouts with Neon Cloud Postgres by using short-lived transactions per item in the news pipeline.
3. **Adaptive Routing (Jobs)**: The V4 orchestrator uses Broad APIs first, then runs a Gap Analysis. Only uncovered target companies are passed to the scraping cascade (Apify T1 $\to$ Browserbase T2 $\to$ Watchlist T3), heavily optimizing Apify and Browserbase compute budgets.

The platform operates across four decoupled architectural planes:

```text
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│     1. INGESTION        │     │     2. PROCESSING       │     │       3. STORAGE        │     │      4. DELIVERY        │
│                         │     │                         │     │                         │     │                         │
│ • Broad Job APIs        │────▶│ • V4 Gap Analyzer       │────▶│ • PostgreSQL (Neon)     │────▶│ • APScheduler Cron      │
│ • Target Career Pages   │     │ • EV / Business Filters │     │ • jobs table            │     │ • Digest Assembler      │
│ • News (RSS/Social)     │────▶│ • TensorMux LLM Scoring │────▶│ • processed_news table  │────▶│ • Resend Email API      │
└─────────────────────────┘     └─────────────────────────┘     └─────────────────────────┘     └─────────────────────────┘
```

---

## 📊 End-to-End System Flowcharts

### 1. Overall System Architecture

```mermaid
flowchart TD
    subgraph Ingestion ["1. Ingestion"]
        JobsIn[Job Sources\nAPIs, ATS, Scraping]
        NewsIn[News Sources\nRSS, Twitter, LinkedIn]
    end
    
    subgraph Processing ["2. AI & Processing"]
        Gap[V4 Gap Analyzer]
        Filter[Filters & Deduplication]
        LLM[TensorMux LLM\nNews Summarization]
    end
    
    subgraph Storage ["3. Storage (PostgreSQL)"]
        DB[(Neon Database\nJobs & News)]
    end
    
    subgraph Delivery ["4. Delivery"]
        Email[Resend API\nDaily Digests]
    end

    JobsIn --> Gap
    Gap --> Filter
    Filter --> DB
    
    NewsIn --> LLM
    LLM --> DB
    
    DB --> Email
```

---

### 2. Jobs Pipeline & V4 Gap Cascade Flowchart

```mermaid
flowchart TD
    Broad[Broad Scrapers\nLinkedIn, Adzuna] --> Gap{Gap Analyzer}
    Gap -->|"Target Not Covered"| T1[Tier 1: Apify]
    Gap -->|"Covered"| Skip[Skip Scrape]
    
    T1 -.->|"Fails (SPA)"| T2[Tier 2: Browserbase]
    
    T1 & T2 --> Classify{Classifier Gate\nBusiness / EV Only}
    Classify -->|"Pass"| Dedup[Fuzzy Deduplication]
    Classify -->|"Fail"| Drop[Discard Job]
    
    Dedup --> DB[(Database)]
```

---

### 3. News Processing & AI Pipeline Flowchart

```mermaid
flowchart LR
    Ingest[Raw News\nn8n, Twitter, LinkedIn] --> Filter[Indian EV\nKeyword Filter]
    Filter -->|"Matched"| LLM[TensorMux LLM\nScore & Summarize]
    Filter -.->|"No Match"| Drop[Discard]
    
    LLM -->|"Score >= 0.40"| DB[(Database)]
```

---

## 🏗️ Architectural Planes & Directory Map

```
Daily-intel/
├── docs/                        # Specifications, PRDs, and architecture logs
│   ├── PRD.md                   # Full product requirements and design constraints
│   ├── architecture-V4.md       # Current production architecture specification
│   ├── architecture-V3.md       # Scraping cascade & budget optimization spec
│   ├── dev-checklist.md         # Granular implementation & bug resolution log
│   └── decisions-log.md         # Historical decisions and trade-off rationale
│
├── intel-system/                # FastAPI application package & backend services
│   ├── app/
│   │   ├── main.py              # Application entrypoint & lifespan management
│   │   ├── config.py            # Pydantic Settings loaded from .env
│   │   ├── database.py          # Async SQLAlchemy engine with SSL & connection pools
│   │   ├── models/              # SQLAlchemy ORM models (Job, TargetCompany, etc.)
│   │   ├── schemas/             # Pydantic data contracts (JobIn, IngestRequest)
│   │   ├── routers/             # HTTP API surface (/ingest, /admin, /health)
│   │   ├── scheduler/           # APScheduler cron jobs & lifecycle runners
│   │   ├── services/
│   │   │   ├── jobs/            # Complete Jobs module (V4 orchestrator, adapters)
│   │   │   │   ├── scrape_orchestrator.py # V4 Broad -> Gap -> T1/T2 pipeline
│   │   │   │   ├── gap_analyzer.py        # Deterministic + LLM company matcher
│   │   │   │   ├── classifier.py          # EV taxonomy & business role matcher
│   │   │   │   ├── job_pipeline.py        # Central savepoint filter & persistence
│   │   │   │   ├── job_digest.py          # Batched email digest generator
│   │   │   │   ├── apify_adapter.py       # Tier 1 Apify actor crawler
│   │   │   │   ├── browserbase_adapter.py # Tier 2 Headless CDP fallback
│   │   │   │   ├── ats_adapters/          # Workday, Darwinbox, Greenhouse, Lever
│   │   │   │   └── free_apis/             # Adzuna, HN, Remotive, YC, LinkedIn
│   │   │   ├── news/            # News module (RSS, Twitter, LinkedIn, LLM)
│   │   │   │   ├── news_pipeline.py       # Chunked LLM summarizer & scorer
│   │   │   │   ├── keyword_filter.py      # Indian EV regex & entity filters
│   │   │   │   ├── twitter.py             # Twitter handle poller
│   │   │   │   ├── linkedin.py            # LinkedIn hashtag news poller
│   │   │   │   └── linkedin_community.py  # Community discussion scraper
│   │   │   ├── digest/          # Multi-section digest assembler & Jinja2 templates
│   │   │   └── email/           # Resend API client for automated email delivery
│   │   └── utils/               # Structured logging (structlog), tracing, LLM client
│   ├── alembic/                 # Database migrations (001_foundation to 006_lifecycle)
│   ├── scripts/                 # Operational seeders, URL testers, diagnostic scripts
│   └── tests/                   # Pytest unit, filtering, and contract test suites
│
├── frontend/                    # Vite + React + Tailwind landing page showcase
├── adzuna/                      # Adzuna integration testing scratchpad
└── experiments/                 # Browserbase & Eaton site extraction experiments
```

---

## 🚦 Implementation Status: Built vs. Roadmap

| Phase | Module / Capability | Status | Implementation Details |
|---|---|---|---|
| **Phase 0 & 1** | **Core Foundation & Infrastructure** | ✅ **Complete** | FastAPI, async SQLAlchemy, Alembic, Neon PostgreSQL, Resend email dispatch, Request ID middleware, structlog. |
| **Phase 2 (V3/V4)** | **Jobs Pipeline & Orchestration** | ✅ **Complete** | V4 orchestration (LinkedIn Jobs Apify + Adzuna broad scrape → Gap Analyzer → T1 Apify / T2 Browserbase cascade), ATS adapters, EV/Business role classifier, 2-layer dedup, 21-day staleness cleaner, 60-job batched emails. |
| **Phase 3** | **News & Social Intelligence** | ✅ **Complete** | Ingestion via n8n RSS webhook, Apify Twitter/X poller, Apify LinkedIn news & community pollers, custom site crawlers, keyword pre-filter, TensorMux `glm-4-7-flash` chunked summarizer, 20-item batched emails. |
| **Phase 4** | **Telegram Integration** | 🚧 *Pending* | Telethon user-API client for group monitoring, session persistence, OCR for media. |
| **Phase 5** | **pgvector Personalization Memory** | 🚧 *Pending* | Vector embeddings table, top-3 past context retrieval before summarization, email click tracking pixel, preference re-ranking. |
| **Phase 6** | **WhatsApp Group Monitoring** | 🚧 *Pending* | Baileys Node.js microservice on dummy account, strict read-only enforcement. |
| **Phase 7** | **LinkedIn Creator Posts via RapidAPI** | 🚧 *Pending* | RapidAPI multi-provider abstraction for tracking specific executive posts. |
| **Phase 8** | **Production Hardening & VPS Deploy** | 🚧 *Pending* | Hetzner CX32 VPS setup, Sentry integration, daily `pg_dump` backups, SPF/DKIM validation. |

---

## ⚙️ Key Technical Contracts

### 1. The `JobIn` Ingestion Contract
Every job source (scrapers, APIs, ATS endpoints) normalizes its payload into `app.schemas.job.JobIn` before passing to `persist_filtered_jobs()`:
```python
class JobIn(BaseModel):
    company: str
    job_title: str
    job_url: str
    location: str | None = None
    remote: bool | None = None
    department: str | None = None
    description: str | None = None
    experience_level: str = "unknown"
    source_type: str
    target_company_id: int | None = None
```

### 2. The Two-Gate Filtering Rule
- **Target Company Sources** (`apify_*`, `ats_*`, `direct_*`): The company is already a verified EV player. Gated purely on **Business / Management / Ops role fit** (`is_business_role()`).
- **Public Broad Sources** (`linkedin`, `adzuna`, `hn`, `yc`, `remotive`): Gated on **both India location match AND explicit EV domain relevance** (`is_ev_relevant()`).

### 3. Deduplication & Email Isolation
- **Hard Deduplication**: SHA-256 `dedup_hash` calculated over normalized `company | title | location | stripped_url`.
- **Duplicate Handling**: When a duplicate hash appears, the database updates `last_seen_at = now()`, extending the job's freshness without creating duplicate rows.
- **Email Deduplication**: Production digests query `WHERE emailed_at IS NULL`. Once sent, `emailed_at` is stamped with UTC timestamp, guaranteeing each posting is emailed exactly once.

---

## 💻 Windows Setup & Local Execution Guide (PowerShell)

Follow these verified, step-by-step instructions to set up, run, and test **Daily Intel** on a fresh Windows machine using **PowerShell**.

> [!IMPORTANT]
> In Windows PowerShell, `curl` is an alias for `Invoke-WebRequest`. **Always use `curl.exe`** as specified below to ensure arguments, HTTP methods, and JSON payloads are processed properly.

---

### Step 1: System Prerequisites

Ensure the following tools are installed:
- **Python 3.12+** ([python.org](https://www.python.org/downloads/)) — Ensure "Add Python to PATH" is checked.
- **Git for Windows** ([git-scm.com](https://git-scm.com/download/win))
- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/)) — Required for local PostgreSQL with pgvector and n8n.
- **Node.js 18+ (Optional)** ([nodejs.org](https://nodejs.org/)) — Only needed if you wish to run the React showcase UI.

---

### Step 2: Clone the Repository & Open PowerShell

```powershell
# Navigate to your workspace directory
Set-Location -Path "$HOME\Projects"

# Clone the repository
git clone https://github.com/dev-infinity101/daily-intel.git

# Move into the project directory
Set-Location -Path "Daily-intel"
```

---

### Step 3: Start Local Infrastructure via Docker

```powershell
# Move to the backend service directory
Set-Location -Path "intel-system"

# Start background services (PostgreSQL 16 + pgvector, n8n)
docker compose up -d
```

> [!TIP]
> Verify containers are healthy:
> ```powershell
> docker ps
> ```
> You should see `postgres` (port 5432) and `n8n` (port 5678) running.

---

### Step 4: Setup Python Virtual Environment

```powershell
# Enable script execution for this PowerShell process if restricted:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process -Force

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Upgrade pip and install all Python dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

### Step 5: Configure Environment Variables

```powershell
# Copy the example environment file
Copy-Item .env.example .env
```

Open `.env` (e.g. `notepad .env` or in VS Code) and set your keys:

```ini
# Database (Local Docker Postgres by default, or Neon Cloud Postgres)
DATABASE_URL=postgresql+asyncpg://intel:intel_dev@localhost:5432/intel

# AI / LLM Engine (TensorMux)
TENSORMUX_API_KEY=your_tensormux_api_key_here
TENSORMUX_MODEL=glm-4-7-flash

# Email Delivery (Resend)
RESEND_API_KEY=re_your_resend_api_key
EMAIL_FROM=Daily Intel <intel@yourdomain.com>
EMAIL_TO=your_email@domain.com

# Scraper Infrastructure (Apify & Browserbase)
APIFY_TOKEN=apify_api_your_token
APIFY_TOKEN_SECONDARY=
BROWSERBASE_API_KEY=your_browserbase_api_key
BROWSERBASE_PROJECT_ID=your_browserbase_project_id

# Ingestion Security & Timezone
INGEST_TOKEN=dev-token
TZ=Asia/Kolkata
```

---

### Step 6: Run Database Migrations & Seed Targets

```powershell
# Apply Alembic schema migrations (001_foundation through 006_job_lifecycle)
python -m alembic upgrade head

# Seed 136 target companies with ATS metadata from CSV
python scripts/seed_from_csv.py
```

---

### Step 7: Start the FastAPI Server

```powershell
uvicorn app.main:app --reload --port 8000
```

- **Interactive API Docs (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Health Check:** [http://localhost:8000/healthz](http://localhost:8000/healthz)
- **n8n Automation Console:** [http://localhost:5678](http://localhost:5678)

---

## 🧪 Testing & Verification Commands (PowerShell `curl.exe`)

Open a **separate PowerShell window**, activate the virtual environment (`.\venv\Scripts\Activate.ps1`), and run these test commands:

### 1. Health & Database Sanity Checks

```powershell
# Basic liveness ping
curl.exe -s http://localhost:8000/healthz

# Database connectivity check
curl.exe -s http://localhost:8000/readyz

# Schema check, total jobs count & active target companies count
curl.exe -s http://localhost:8000/admin/jobs/db-check
```

---

### 2. Run Automated Pytest Suite

```powershell
# Run all unit, classifier, and adapter tests (excluding live network calls)
pytest tests/ -v -m "not integration"
```

---

### 3. Jobs Module Testing (Scrapers, Filtering & Email Digest)

```powershell
# A. Single-company test scrape (e.g. Tata Motors)
curl.exe -X POST "http://localhost:8000/admin/jobs/scrape-now?company=tatamotors"

# B. Immediate Adzuna API job scrape
curl.exe -X POST http://localhost:8000/admin/jobs/adzuna-jobs

# C. Immediate LinkedIn Jobs Apify scrape
curl.exe -X POST http://localhost:8000/admin/jobs/linkedin-jobs

# D. Run Gap Analyzer (Checks DB coverage, scrapes only uncovered targets)
curl.exe -X POST http://localhost:8000/admin/jobs/gap-analyzer

# E. Preview rendered HTML for unsent jobs (without sending email)
curl.exe -s http://localhost:8000/admin/jobs/preview

# F. Dispatch the Jobs Digest email via Resend
curl.exe -X POST http://localhost:8000/admin/jobs/digest-now

# G. Inspect Scraper Metrics & Monthly Budget Utilization
curl.exe -s http://localhost:8000/admin/jobs/metrics
```

---

### 4. News Module Testing (Social Intelligence & AI Summarization)

```powershell
# A. Ingest a test RSS item via n8n Webhook
curl.exe -X POST http://localhost:8000/ingest/news/n8n-webhook `
  -H "Content-Type: application/json" `
  -d '{\"title\":\"Tata Motors expands EV charging network in Pune\",\"link\":\"https://example.com/ev-news\",\"content\":\"Tata Motors announced a major expansion of EV charging stations across Maharashtra.\"}'

# B. Test the Indian EV Keyword Pre-Filter
curl.exe -X POST http://localhost:8000/admin/news/keywords/test `
  -H "Content-Type: application/json" `
  -d '{\"text\":\"Ola Electric launches new battery gigafactory in Tamil Nadu.\"}'

# C. Trigger immediate Apify Twitter/X Scraper
curl.exe -X POST http://localhost:8000/admin/news/twitter/trigger-now

# D. Trigger immediate Apify LinkedIn News Scraper
curl.exe -X POST http://localhost:8000/admin/news/linkedin/trigger-now

# E. Trigger TensorMux LLM Summarization & Scoring on raw items
curl.exe -X POST http://localhost:8000/admin/news/process-now

# F. Preview & Send the News Digest Email
curl.exe -s http://localhost:8000/admin/news/preview
curl.exe -X POST http://localhost:8000/admin/news/digest-now
```

---

## 🌐 Optional: Run the Frontend Showcase UI

```powershell
# In a separate PowerShell window:
Set-Location -Path "frontend"

# Install NPM dependencies
npm install

# Start Vite development server
npm run dev
```
Open [http://localhost:5173](http://localhost:5173) in your browser.

---

## 🪟 Windows Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| `running scripts is disabled on this system` | PowerShell script execution restriction | Run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` |
| `curl : A parameter cannot be found...` | PowerShell aliasing `curl` to `Invoke-WebRequest` | Always type **`curl.exe`** instead of `curl` |
| Port 5432 conflict | Native Postgres already running on Windows | Change host port mapping in `docker-compose.yml` to `"5433:5432"` and update `DATABASE_URL` in `.env` to port `5433` |
| SSL Error with Neon Cloud Postgres | Local vs Cloud connection handling | Local Docker auto-disables SSL; Cloud Neon connections auto-enforce SSL in `app/database.py` |

---

## 📜 Architectural Decisions & References

- `docs/PRD.md` — Product specifications, persona requirements, and constraints.
- `docs/architecture-V4.md` — Current production design with TensorMux, V4 Gap Scraper, and News engine.
- `docs/architecture-V3.md` — Deep dive into Apify/Browserbase quotas, extraction fixes, and adaptive routing.
- `docs/dev-checklist.md` — Historical execution log, session notes, and bug resolution history.
- `docs/decisions-log.md` — Strategic architecture decisions and provider choices.
