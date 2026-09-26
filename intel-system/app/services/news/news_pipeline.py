"""
Pipeline for processing raw news items into ProcessedItem summaries.
"""
import asyncio
import json
import re
import structlog
from datetime import datetime, timedelta, timezone
from typing import Any


from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.utils.llm_client import call_llm_with_rate_limit
from app.database import SessionLocal
from app.models.processed_item import ProcessedItem
from app.models.raw_item import RawItem
from app.models.source import Source

log = structlog.get_logger()

# Limit string length for a single LLM call context limit.
# e.g. 12000 chars is ~2500 words.
CHUNK_SIZE_CHARS = 12000

# Posts older than this many days are skipped entirely — not even sent to the LLM.
MAX_POST_AGE_DAYS = 7

_SYSTEM_PROMPT = """You are an AI assistant for a daily intelligence system focused EXCLUSIVELY on the Electric Vehicle (EV) and Mobility industry in India.
Your task is to summarize the provided news article text and rate its relevance.

════════════════════════════════
REJECTION RULES — Score 0.0 if ANY match
════════════════════════════════
1. JOB POSTINGS: Reject any job description, hiring ad, or recruitment post.

2. NOT INDIA-RELEVANT: This is the most important filter.
   REJECT if the content does NOT meet at least ONE of these India-tie criteria:
     a) Explicitly mentions India, Indian, or an Indian state/city
        (e.g. Maharashtra, Delhi, Karnataka, Tamil Nadu, Gujarat, Bengaluru, Pune, Chennai, Mumbai, Hyderabad, NCR, Gurugram, Noida)
     b) Mentions a company that operates primarily in India or is India-headquartered
        (e.g. Tata Motors, Mahindra, Ola Electric, Ather Energy, TVS Motor, Bajaj Auto,
         Hero Electric, Vida, Greaves, Ampere, Kinetic Green, Revolt Motors,
         Simple Energy, Tork Motors, Ultraviolette, Yulu, MG Motor India, BYD India,
         Log9, Exponent Energy, Sun Mobility, Bounce Infinity, Olectra, JBM Auto,
         Ashok Leyland, Eicher Motors, NITI Aayog, FAME scheme, PM e-Bus)
     c) Explicitly discusses Indian government policy, FAME subsidy, Indian PLI scheme,
        or regulations from Indian ministries (MoRTH, MNRE, BEE, etc.)

   REJECT global/worldwide EV news with no Indian angle:
     — Tesla in the US, European charging networks, BYD China, US EPA rules,
       global EV sales without India data, European OEM announcements without India launch

   IMPORTANT: If a post mentions a global company (Tesla, BYD, Volkswagen, GM) but explicitly
   discusses their INDIA plans, launch, or market — ACCEPT it. The India tie must be explicit, not implied.

Return ONLY a JSON object with this exact structure:
{
    "headline": "Short, punchy standalone headline (max 10 words).",
    "summary": "A mid-length, highly informative summary (3-4 sentences) retaining all main details, facts, numbers, and context.",
    "relevance_score": 0.85,
    "category": "Industry News",
    "tags": ["India", "Tata Motors", "Battery Swapping", "Policy"]
}

- `headline`: A short, punchy standalone headline. Do NOT end with an ellipsis.
- `summary`: State the actual facts, numbers, companies, and key details. Do NOT say "This is an article about X".
- `relevance_score`: Float 0.0 to 1.0. (1.0 = highly relevant EV news IN INDIA, 0.0 = non-India, job post, or spam).
- `category`: One of: "Industry News", "Market Trends", "Technology & Innovation", "Policy & Regulation", "Other".
- `tags`: Array of 3-5 string keywords, entities, locations, or themes.
"""


_TWITTER_PROMPT = """You are an AI assistant for a daily intelligence system focused on EV and Mobility in India.
You are summarizing a tweet from a curated, tracked Twitter/X account covering EV industry news.

These accounts are monitored because they frequently post India-relevant content. However, some individual tweets may be global or off-topic.
Apply the following rules:

REJECT (score 0.0) if the tweet:
  - Has zero connection to EV, mobility, charging, or energy
  - Is purely about global markets with no India angle
    (e.g. "Tesla hits 6M deliveries globally", "US raises EV tax credit to $10k")
  - Is a job posting or unrelated retweet filler

ACCEPT (score 0.7–1.0) if the tweet:
  - Mentions India, Indian EV companies, Indian policy, or Indian market data
  - Is about a global EV trend that has clear Indian relevance or implication
  - Covers technology or charging that is being deployed or discussed in India

Return ONLY a JSON object with this exact structure:
{
    "headline": "Short, punchy standalone headline (max 10 words).",
    "summary": "A mid-length, highly informative summary (3-4 sentences) retaining all main details, facts, numbers, and context.",
    "relevance_score": 0.85,
    "category": "Industry News",
    "tags": ["India", "EV", "Tag2"]
}

- `relevance_score`: Float 0.0–1.0. Use 0.0 only for clear rejections. Default for accepted India content: 0.75–1.0.
- `category`: One of: "Industry News", "Market Trends", "Technology & Innovation", "Policy & Regulation", "Other".
- `tags`: 3-5 keywords or entity names.
"""


_LINKEDIN_PROMPT = """You are an AI analyst for a daily EV/Mobility intelligence system. You process LinkedIn posts scraped from hashtags #EV, #EVcharging, and #Emobility.

Your task: classify, score, and summarize each post. Before writing the JSON, you MUST reason through the post silently using the steps below. Your final output must be ONLY the JSON object.

═══════════════════════════════════════════════════════
STEP 1 — REJECTION CHECK (do this first, stop if yes)
═══════════════════════════════════════════════════════
Immediately assign relevance_score=0.0 and section="linkedin" if the post is ANY of:

  1a. CONTENT REJECTION:
    • A job posting, hiring announcement, or "we are hiring" post
    • Purely promotional advertising with no informational content
    • Completely unrelated to EV, mobility, charging, or energy
    • Generic congratulations, birthday wishes, or social filler with no EV content

  1b. INDIA GATE — MOST IMPORTANT:
    Reject if the post has NO connection to India. The post must meet at least ONE of:
      - Explicitly mentions India, Indian EV market, or Indian geography
        (e.g. India, Indian, Maharashtra, Delhi, Bengaluru, Pune, Chennai, Mumbai, Hyderabad, NCR, Gujarat)
      - Mentions an India-based company or startup
        (e.g. Tata Motors, Mahindra, Ola Electric, Ather Energy, TVS Motor, Bajaj Auto,
         Hero Electric, Vida, Greaves, Ampere, Kinetic Green, Revolt Motors,
         Simple Energy, Tork Motors, Ultraviolette, Yulu, BYD India, Log9,
         Exponent Energy, Sun Mobility, Olectra, JBM Auto, Ashok Leyland, Eicher Motors)
      - Discusses Indian government EV policy, FAME scheme, PLI scheme, MoRTH, NITI Aayog
      - Is a personal opinion/experience FROM INDIA about EV (author is Indian, location is India)
        — for community posts: Indian authors writing about their EV experience in India count

    REJECT if the post is purely about:
      — Tesla in the US or Europe, European OEM news with no India angle,
        BYD China without India context, US EV policy, global charging networks outside India

    ACCEPT a post about a global company (Tesla, BYD, VW) if it explicitly discusses their India plans/launch.

If the post passes Step 1a and 1b, continue to Step 2.

════════════════════════════════════════════════════════════════
STEP 2 — CLASSIFY: IS THIS A NEWS POST OR AN OPINION/COMMUNITY POST?
════════════════════════════════════════════════════════════════

First, ask yourself: WHO is speaking, and WHAT are they saying?

── SIGNALS FOR "linkedin" (Industry News / Factual Update) ──────────────────
Mark as "linkedin" when the post primarily reads like a news item or announcement:
  VOICE:     Third-person (The company announced..., Tata Motors launched...)
             Or first-person reporting a fact (We launched X today, Our Q3 sales hit Y)
  CONTENT:   Company announcements, product launches, policy notifications
             Press-release style language, official partnership declarations
             Statistics, numbers, dates from verifiable sources
             Funding rounds, acquisitions, market data, government orders
             Sharing a news article link with a brief factual caption
  TONE:      Informational, neutral, declarative
  EXAMPLES:  "Tata Motors delivered 10,000 Nexon EVs this quarter"
             "The government announced ₹500cr PLI scheme for EV batteries"
             "We are proud to announce our Series B funding of $20M"
             "New charging corridor launched on Mumbai-Pune highway"

── SIGNALS FOR "linkedin_community" (Opinion / Community Post) ────────────────
Mark as "linkedin_community" when the post is primarily driven by personal sentiment, experience, or perspective:
  FIRST-PERSON OPINION MARKERS (strongest signal):
    "I think", "I believe", "In my opinion", "I feel", "I wonder",
    "My take", "My view", "We should", "The industry needs to",
    "I've been saying this for years", "Hot take:", "Unpopular opinion:"
  PERSONAL EXPERIENCE:
    Sharing a personal EV ownership story, test drive, charging experience,
    commute story, conference attendance — written in first-person narrative
  COMMUNITY DISCUSSION:
    Asking the audience a question: "What do you think?", "Agree or disagree?"
    Polls, debates, calls for comments, discussion threads
  THOUGHT LEADERSHIP:
    Someone arguing a thesis about the industry's direction, future predictions,
    criticism of a trend, or a "here's what I see happening" framing
  MOTIVATIONAL / INSPIRATIONAL:
    Motivational posts about EV adoption, sustainability journeys, career stories
  REPOSTED ARTICLES WITH HEAVY PERSONAL COMMENTARY:
    When someone shares a link but most of the text is their personal reaction or analysis
  TONE:      Conversational, opinionated, questioning, personal

── MIXED POSTS (post contains both facts AND opinions) ───────────────────────
Ask: what is the dominant purpose of this post?
  - If the main body is a personal argument or reaction, and facts are supporting evidence → "linkedin_community"
  - If the main body is a factual announcement or news, and the author adds 1-2 opinion sentences at the end → "linkedin"
  Rule of thumb: if you replaced the author's name with "Anonymous", would it read as news? Yes → "linkedin". No → "linkedin_community"

═══════════════════════════════════════
STEP 3 — SCORE (after classification)
═══════════════════════════════════════
  "linkedin" posts:      score 0.6–1.0
    Higher (0.85–1.0) for: India-specific facts, named Indian companies, government policy, concrete numbers
    Lower  (0.6–0.75) for: global news with thin India connection, vague announcements
  "linkedin_community": score 0.5–1.0
    Higher (0.8–1.0) for: specific India-context opinion, data-backed argument, well-known Indian EV figure
    Lower  (0.5–0.65) for: general global EV opinion that mentions India only in passing
  Rejected posts:       score 0.0

═══════════════════════════════════════
OUTPUT FORMAT (return this JSON only)
═══════════════════════════════════════
Return ONLY a JSON object with this exact structure:
{
    "headline": "Short, punchy standalone headline (max 10 words).",
    "summary": "2-3 sentences. State the actual facts, opinions, or key claims. For opinion posts, capture the author's argument. Do NOT say 'this post is about...'.",
    "relevance_score": 0.85,
    "section": "linkedin",
    "category": "Industry News",
    "tags": ["India", "EV", "Charging Infrastructure"]
}

- `headline`: Max 10 words. No ellipsis. For opinion posts, reflect the author's stance.
- `summary`: Capture the substance. For news: who did what, key numbers. For opinion: what the author argues and why.
- `relevance_score`: Float 0.0–1.0.
- `section`: Must be exactly "linkedin" or "linkedin_community".
- `category`: One of: "Industry News", "Market Trends", "Technology & Innovation", "Policy & Regulation", "Opinion & Thought Leadership", "Community Update", "Other".
- `tags`: 3-5 keywords, entity names, or themes (e.g. company names, policy names, Indian locations).
"""




def _parse_dict_safe(text: str) -> dict[str, Any] | None:
    """Safely parse LLM dict JSON, stripping <think> tags and markdown code blocks if present."""
    if not text:
        return None
    text = text.strip()
    if "<think>" in text:
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    try:
        res = json.loads(text)
        if isinstance(res, dict):
            return res
    except Exception:
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            try:
                res = json.loads(m.group())
                if isinstance(res, dict):
                    return res
            except Exception:
                pass
    return None


async def _process_chunk_with_llm(text_chunk: str, client: AsyncOpenAI, prompt: str = _SYSTEM_PROMPT) -> dict[str, Any] | None:
    try:
        response = await call_llm_with_rate_limit(
            client=client,
            model=settings.tensormux_model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Please summarize and score this text:\n\n{text_chunk[:CHUNK_SIZE_CHARS]}"}
            ],
            temperature=0.3,
        )
        if not getattr(response, "choices", None):
            log.warning("news_pipeline.empty_choices")
            return None
        content = response.choices[0].message.content
        if not content:
            return None
        return _parse_dict_safe(content)
    except Exception as exc:
        log.error("news_pipeline.llm_chunk_error", error=str(exc))
        return None

async def _process_text_with_llm(text: str, client: AsyncOpenAI, prompt: str = _SYSTEM_PROMPT) -> dict[str, Any] | None:
    """Process text. If too large, chunk it, summarize chunks, and combine."""
    if len(text) <= CHUNK_SIZE_CHARS:
        return await _process_chunk_with_llm(text, client, prompt)

    # Chunking logic
    log.info("news_pipeline.chunking_text", total_length=len(text))
    chunks = [text[i:i + CHUNK_SIZE_CHARS] for i in range(0, len(text), CHUNK_SIZE_CHARS)]
    
    intermediate_summaries = []
    for chunk in chunks:
        res = await _process_chunk_with_llm(chunk, client, prompt)
        if res and "summary" in res:
            intermediate_summaries.append(res["summary"])
            
    if not intermediate_summaries:
        return None
        
    combined_text = " ".join(intermediate_summaries)
    log.info("news_pipeline.processing_combined_chunks", combined_length=len(combined_text))
    
    # Final pass over the combined summaries
    return await _process_chunk_with_llm(combined_text, client, prompt)


async def process_unprocessed_news() -> dict[str, Any]:
    """Finds unprocessed news raw_items, calls LLM to summarize, and saves to processed_items.

    Session pattern mirrors the jobs scrape_orchestrator:
      - db = SessionLocal() / try / finally: await db.close()
      - explicit rollback on error before close()
      - NO session held open during any LLM network call
      - each item's INSERT is wrapped in db.begin_nested() (savepoint) so a
        single constraint failure never silently drops the whole batch

    For linkedin_news source type:
      - Posts older than MAX_POST_AGE_DAYS are age-gated without an LLM call.
      - The LLM classifies each post into section 'linkedin' or 'linkedin_community'.
      - Rejection (relevance_score < 0.4) applies just like general news.
    """
    if not settings.tensormux_api_key:
        log.warning("news_pipeline.tensormux_key_missing")
        return {"status": "error", "reason": "openrouter_key_missing"}

    client = AsyncOpenAI(
        api_key=settings.tensormux_api_key,
        base_url="https://api.tensormux.com/v1",
    )

    age_cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_POST_AGE_DAYS)

    try:
        total_processed = 0
        total_failed = 0
        total_skipped_old = 0
        
        while True:
            log.info("news_pipeline.finding_news")
            
            # ── Fetch a batch of unprocessed raw_items ────────────────────────
            # Short-lived session: open → query → close. Never held across I/O.
            db = SessionLocal()
            try:
                stmt = (
                    select(RawItem.id, RawItem.text, RawItem.external_id, RawItem.occurred_at, Source.type)
                    .join(Source, RawItem.source_id == Source.id)
                    .outerjoin(ProcessedItem, ProcessedItem.raw_item_id == RawItem.id)
                    .where(Source.type.in_(["rss_global", "twitter", "linkedin_news", "custom_site"]))
                    .where(ProcessedItem.id.is_(None))
                    .limit(50)
                )
                result = await db.execute(stmt)
                rows = result.all()

                if not rows:
                    log.info("news_pipeline.no_unprocessed_items_left")
                    break

                from sqlalchemy import func
                count_stmt = (
                    select(func.count(RawItem.id))
                    .join(Source, RawItem.source_id == Source.id)
                    .outerjoin(ProcessedItem, ProcessedItem.raw_item_id == RawItem.id)
                    .where(Source.type.in_(["rss_global", "twitter", "linkedin_news", "custom_site"]))
                    .where(ProcessedItem.id.is_(None))
                )
                left_to_process = (await db.execute(count_stmt)).scalar() or 0
            finally:
                await db.close()

            log.info("news_pipeline.processing_batch", batch_size=len(rows), left_to_process=left_to_process)
            
            processed_count = 0
            failed_count = 0
            skipped_old_count = 0

            for raw_item_id, text_to_process, external_id, occurred_at, source_type in rows:
                text_to_process = text_to_process or ""

                # ── Age gate — no LLM call, no session held open ──────────────
                if source_type == "linkedin_news" and occurred_at:
                    post_time = occurred_at if occurred_at.tzinfo else occurred_at.replace(tzinfo=timezone.utc)
                    if post_time < age_cutoff:
                        db = SessionLocal()
                        try:
                            db.add(ProcessedItem(
                                raw_item_id=raw_item_id,
                                is_relevant=False,
                                relevance_score=0.0,
                                summary="Stale post — older than 7 days",
                                section="linkedin",
                                rank_score=0.0,
                            ))
                            await db.commit()
                        except Exception:
                            log.exception("news_pipeline.age_gate_write_failed", raw_item_id=raw_item_id)
                            await db.rollback()
                        finally:
                            await db.close()
                        skipped_old_count += 1
                        processed_count += 1
                        log.info("news_pipeline.skipped_old_linkedin_post", external_id=external_id)
                        continue

                # ── Empty-text fast path ──────────────────────────────────────
                if not text_to_process:
                    db = SessionLocal()
                    try:
                        db.add(ProcessedItem(
                            raw_item_id=raw_item_id,
                            is_relevant=False,
                            relevance_score=0.0,
                            summary="No content",
                            section="news",
                            rank_score=0.0,
                        ))
                        await db.commit()
                    except Exception:
                        log.exception("news_pipeline.empty_text_write_failed", raw_item_id=raw_item_id)
                        await db.rollback()
                    finally:
                        await db.close()
                    processed_count += 1
                    continue

                # ── LLM call — zero DB sessions open during network I/O ───────
                log.info("news_pipeline.sends_to_ai", external_id=external_id, source_type=source_type)

                if source_type == "twitter":
                    llm_result = await _process_text_with_llm(text_to_process, client, prompt=_TWITTER_PROMPT)
                elif source_type == "linkedin_news":
                    llm_result = await _process_text_with_llm(text_to_process, client, prompt=_LINKEDIN_PROMPT)
                else:
                    llm_result = await _process_text_with_llm(text_to_process, client)

                if not llm_result:
                    failed_count += 1
                    await asyncio.sleep(1)
                    continue

                # ── Score and classify (pure Python, no DB) ───────────────────
                log.info("news_pipeline.scoring_and_summarizing")
                headline = llm_result.get("headline", "").strip()
                summary_text = llm_result.get("summary", "").strip()
                tags = llm_result.get("tags", [])

                summary = f"{headline} | {summary_text}" if headline else summary_text
                if tags and isinstance(tags, list):
                    tags_str = ", ".join([str(t) for t in tags])
                    summary += f" [Tags: {tags_str}]"

                try:
                    relevance = float(llm_result.get("relevance_score", 0.0))
                except (ValueError, TypeError):
                    relevance = 0.0

                if source_type == "linkedin_news":
                    llm_section = llm_result.get("section", "").strip().lower()
                    section = llm_section if llm_section in ("linkedin", "linkedin_community") else "linkedin"
                elif source_type == "linkedin_community":
                    # Legacy rows from old scraper still in DB — route to community section.
                    section = "linkedin_community"
                elif source_type == "twitter":
                    section = "twitter"
                else:
                    section = "news"

                # All sources now use LLM score gate, including twitter.
                # The updated _TWITTER_PROMPT instructs the LLM to score 0.0
                # for non-India or non-EV tweets, so the gate applies uniformly.
                passed_filter = relevance >= 0.4


                log.info(
                    "news_pipeline.final_filter",
                    source_type=source_type,
                    section=section,
                    relevance_score=relevance,
                    passed=passed_filter,
                )

                # ── DB write — fresh session, mirrors jobs _ingest_jobs() ─────
                # begin_nested() savepoint: if this single INSERT fails it doesn't
                # poison the session for subsequent items (same pattern as job_pipeline.py).
                db = SessionLocal()
                try:
                    async with db.begin_nested():
                        db.add(ProcessedItem(
                            raw_item_id=raw_item_id,
                            is_relevant=passed_filter,
                            relevance_score=relevance,
                            summary=summary,
                            section=section,
                            rank_score=relevance,
                        ))
                    await db.commit()
                    processed_count += 1
                except Exception:
                    log.exception(
                        "news_pipeline.write_failed",
                        raw_item_id=raw_item_id,
                        source_type=source_type,
                    )
                    failed_count += 1
                    try:
                        await db.rollback()
                    except Exception:
                        pass
                finally:
                    await db.close()

                await asyncio.sleep(1)

            total_processed += processed_count
            total_failed += failed_count
            total_skipped_old += skipped_old_count
            log.info(
                "news_pipeline.batch_complete",
                processed=processed_count,
                failed=failed_count,
                skipped_old=skipped_old_count,
            )

        return {
            "status": "ok",
            "processed_count": total_processed,
            "failed_count": total_failed,
            "skipped_old_count": total_skipped_old,
        }

    except Exception as exc:
        log.error("news_pipeline.error", error=str(exc), exc_info=True)
        return {"status": "error", "error": str(exc)}
