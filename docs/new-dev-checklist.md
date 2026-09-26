# New Dev Checklist — WhatsApp & Telegram Modules

> **Scope**: Completion of Phase 6 (WhatsApp Module) and Phase 4 (Telegram Module).

## Module 1: WhatsApp (Phase 6)
- [x] Create Baileys Node.js Microservice scaffolding (`package.json`, `index.js`, `Dockerfile`).
- [x] Configure `docker-compose.yml` to include `whatsapp_service` and `baileys_auth_info` volume.
- [x] Add `POST /ingest/whatsapp` route to FastAPI.
- [x] **Procure dummy phone number/SIM card** for WhatsApp registration.
- [x] **Register WhatsApp account** on a physical device or emulator using the dummy number.
- [ ] **Start `whatsapp_service` container**, read the QR code from the Docker logs, and scan it via the WhatsApp app to authenticate the Baileys session.
- [ ] **Identify Group JIDs**: Use the `[DISCOVERY]` logs to identify the JID (e.g., `1234567890-1234@g.us`) for target WhatsApp groups by sending a test message.
- [ ] **Seed Database**: Add the identified JIDs to the `sources` table in the database so the system knows to track them.
- [ ] **Verify End-to-End**: Send a message to the group and confirm it reaches the `/ingest/whatsapp` endpoint and processes successfully into `raw_items`.
- [ ] **Implement OCR fallback (Optional)**: If images are shared in groups, route them to TensorMux or Gemini Vision for text extraction before insertion.

## Module 2: Telegram (Phase 4)
- [ ] **Get API Credentials**: Set up a Telegram API ID and Hash (from `my.telegram.org`) for a dedicated dummy Telegram account.
- [ ] **Build Service**: Create a Telethon client sidecar or APScheduler task in Python.
- [ ] **Persistence**: Implement `.session` file persistence to survive container restarts.
- [ ] **Listeners**: Listen for `events.NewMessage(chats=TARGET_GROUPS)`.
- [ ] **Ingestion**: POST incoming text messages to `/ingest` with `X-Source-Type: telegram_channel`.
- [ ] **Seed Database**: Procure Group IDs/Usernames for target Telegram sources and seed the `sources` table.
- [ ] **Verify End-to-End**: Send a message to the Telegram group and ensure it reaches the database.

## Recent Changes Summary

### 1. Database & Schema Migration
- **Processed Items Tracking (`007_processed_item_emailed_at.py`, `models/processed_item.py`)**: Added `emailed_at` (nullable `TIMESTAMPTZ`) column and index `idx_processed_emailed_at` to `processed_items`. Guarantees news and social items are never resent across recurring digest runs.

### 2. Digest Assembler & Templates
- **Digest Deduplication & Twitter Integration (`services/digest/assembler.py`, `templates/news.html.j2`)**:
  - Filtered `fetch_news_items()` to only include records where `emailed_at IS NULL`.
  - Added `stamp_emailed_at()` to stamp timestamp immediately upon successful email dispatch.
  - Added `twitter` ('Twitter Updates') into digest section filtering, display order, and HTML template headers.

### 3. News Ingestion & Processing Pipeline
- **Transaction Safety & Performance (`services/news/news_pipeline.py`)**:
  - Implemented short-lived database sessions: Session is opened solely for fetching unprocessed items and closed during external LLM inference, preventing connection pool exhaustion and transaction locks.
  - Added isolated `begin_nested()` savepoints for atomic writes per processed item (mirroring the jobs pipeline pattern).
  - Added stale item skip guard (`skipped_old_count`) to ignore items beyond the ingestion window.
  - Standardized LLM prompts (`_TWITTER_PROMPT`, `_LINKEDIN_PROMPT`) with strict EV and India-relevance rules (scoring 0.0 for non-India and hiring posts) and enforced a uniform `>= 0.4` relevance threshold.
- **LinkedIn Consolidation (`services/news/linkedin.py`, `services/news/linkedin_community.py`, `routers/news_admin.py`)**:
  - Removed standalone `linkedin_community.py` scraper and merged logic into `linkedin.py` and `news_pipeline.py`.
  - Dynamic classification: The LLM categorizes scraped LinkedIn hashtag posts directly into `"linkedin"` (Industry Updates) or `"linkedin_community"` (Community Updates).
- **Twitter Polling Improvements (`services/news/twitter.py`, `config.py`)**:
  - Switched Apify actor to `apidojo/twitter-profile-scraper`.
  - Added multi-format date parser `_parse_tweet_date()` supporting ISO, Twitter standard, and RFC 2822 timestamps.
  - Added support for handle overrides and date range parameters (`start_date`, `end_date`).

### 4. Job Scrapers & Extraction Enhancements
- **Direct SPA JSON Parsing (`services/jobs/apify_adapter.py`)**: Checks for embedded SPA JSON (`extract_embedded_json()`) directly from raw HTML before falling back to text/markdown, maximizing extraction accuracy.
- **Lowered Extraction Payload Threshold (`services/jobs/apify_adapter.py`)**: Reduced character threshold from 2000 to 500 characters to prevent premature escalation to T2 Browserbase on concise career pages.
- **Regex & Context Window Expansion (`services/jobs/extraction_utils.py`)**:
  - Broadened `_JOB_LINK_RE` pattern to recognize markdown link syntax `](...)` and additional keywords (`req`, `opportun`, `detail`, `listing`, `board`, `apply`, `posting`).
  - Increased `REGION_WINDOW` from 40,000 to 60,000 characters.
  - Set explicit `temperature=0.01` across extraction and gap analyzer LLM calls for JSON stability.

### 5. V4 Scrape Pipeline & Hiring Posts Adapter (T4)
- **Phase 1b LinkedIn Hiring Posts Adapter (`services/jobs/free_apis/linkedin_posts_hiring.py`)**:
  - Implemented T4 scraper using Apify actor `harvestapi/linkedin-post-search` targeting `'"hiring" "EV" "india"'` over the past 7 days (max 50 posts).
  - Normalizes LinkedIn post objects to `JobIn` schemas with `source_type="linkedin_posts_hiring"`, routing through `persist_filtered_jobs()`.
- **5-Phase V4 Orchestration (`services/jobs/scrape_orchestrator.py`, `routers/admin.py`)**:
  - Upgraded `orchestrate_scrape_v4` to sequence: Phase 1a (LinkedIn Jobs) → Phase 1b (LinkedIn Posts Hiring) → Phase 2 (Adzuna) → Phase 3 (Gap Analysis) → Phase 4 (Targeted T1/T2).
  - Added `POST /admin/jobs/lkd-hiring-posts` admin endpoint to execute Phase 1b independently.

### 6. Job Pipeline Filtering & Role Classification
- **Adzuna Category Pre-filtering (`services/jobs/job_pipeline.py`)**: Automatically filters out Adzuna jobs sourced via "EV charging" whose department is not business/sales before DB writes.
- **Classifier Refinements (`services/jobs/classifier.py`)**:
  - Added trainer, educator, training, instructor, and teacher to `_UNRELATED_BUSINESS_ROLES`.
  - Enforced explicit rejection (`role_status="unrelated_business_role"`) for titles matching unrelated business roles.

### 7. Model Configuration & Testing
- **Model Switch & Rate Limits (`config.py`, `utils/llm_client.py`)**:
  - Default TensorMux model set to `glm-4-7-flash`.
  - Adjusted rate limits in `llm_client.py` for `glm-4-7-flash` (RPM 60.0, TPM 500,000.0).
- **Admin Route Fixes (`routers/admin.py`, `routers/news_admin.py`)**: Cleaned up deprecated news routes and fixed monthly total calculations.
- **Test Suites (`tests/test_groq_smoke.py`, `tests/test_linkedin_posts_hiring.py`, `tests/test_adzuna_pipeline.py`, `tests/test_pipeline_filtering.py`)**:
  - Added Groq smoke tests for JSON parsing, ranking, and one-line summaries.
  - Added unit tests for LinkedIn hiring post date filtering and schema normalization.
  - Added pipeline tests for Adzuna department filtering and trainer role rejection.


