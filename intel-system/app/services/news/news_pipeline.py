"""
Pipeline for processing raw news items into ProcessedItem summaries.
"""
import asyncio
import json
import structlog
from typing import Any

from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import SessionLocal
from app.models.processed_item import ProcessedItem
from app.models.raw_item import RawItem
from app.models.source import Source

log = structlog.get_logger()

# Limit string length for a single LLM call context limit.
# e.g. 12000 chars is ~2500 words.
CHUNK_SIZE_CHARS = 12000

_SYSTEM_PROMPT = """You are an AI assistant for a daily intelligence system focused on the Electric Vehicle (EV) and Mobility industry in India.
Your task is to summarize the provided news article text and rate its relevance.

CRITICAL REJECTION RULES (Score 0.0 if any match):
1. JOB POSTINGS: If the text is a job description, hiring ad, or recruitment post, reject it immediately. This is the NEWS module, not the jobs module.
2. EXPLICITLY FOREIGN LOCAL NEWS: Reject news that is strictly about local foreign policies or local foreign events (e.g., "California mandates X", "UK city builds chargers"). However, ACCEPT general global industry trends, major global company news (e.g., Tesla updates), and anything mentioning India.

Return ONLY a JSON object with this exact structure:
{
    "headline": "Exact original headline, or slightly shortened (max 15 words).",
    "summary": "A mid-length, highly informative summary (3-4 sentences) retaining all main details, facts, numbers, and context.",
    "relevance_score": 0.85, 
    "category": "Industry News",
    "tags": ["India", "Tata Motors", "Battery Swapping", "Policy"]
}

- `headline`: Use the original title of the news article as it appears in the text. You may shorten it slightly if it is excessively long, but keep the original wording as much as possible.
- `summary`: A rich, mid-length news description. Do NOT just say "This is an article about X". State the actual facts, numbers, companies, and key details.
- `relevance_score`: Float 0.0 to 1.0. (1.0 = highly relevant EV news in India, 0.0 = job post, non-India, or spam).
- `category`: Must be one of: "Industry News", "Market Trends", "Technology & Innovation", "Policy & Regulation", "Other".
- `tags`: Array of 3-5 string keywords, entities, locations, or themes to be used for future semantic vector searches and personalization.
"""

def _parse_dict_safe(text: str) -> dict[str, Any] | None:
    """Safely parse LLM dict JSON, stripping markdown code blocks if present."""
    text = text.strip()
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
        import re
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            try:
                res = json.loads(m.group())
                if isinstance(res, dict):
                    return res
            except Exception:
                pass
    return None

async def _process_chunk_with_llm(text_chunk: str, client: AsyncOpenAI) -> dict[str, Any] | None:
    try:
        response = await client.chat.completions.create(
            model=settings.openrouter_model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": f"Please summarize and score this text:\n\n{text_chunk[:CHUNK_SIZE_CHARS]}"}
            ],
            temperature=0.3,
        )
        content = response.choices[0].message.content
        if not content:
            return None
        return _parse_dict_safe(content)
    except Exception as exc:
        log.error("news_pipeline.llm_chunk_error", error=str(exc))
        return None

async def _process_text_with_llm(text: str, client: AsyncOpenAI) -> dict[str, Any] | None:
    """Process text. If too large, chunk it, summarize chunks, and combine."""
    if len(text) <= CHUNK_SIZE_CHARS:
        return await _process_chunk_with_llm(text, client)

    # Chunking logic
    log.info("news_pipeline.chunking_text", total_length=len(text))
    chunks = [text[i:i + CHUNK_SIZE_CHARS] for i in range(0, len(text), CHUNK_SIZE_CHARS)]
    
    intermediate_summaries = []
    for chunk in chunks:
        res = await _process_chunk_with_llm(chunk, client)
        if res and "summary" in res:
            intermediate_summaries.append(res["summary"])
            
    if not intermediate_summaries:
        return None
        
    combined_text = " ".join(intermediate_summaries)
    log.info("news_pipeline.processing_combined_chunks", combined_length=len(combined_text))
    
    # Final pass over the combined summaries
    return await _process_chunk_with_llm(combined_text, client)


async def process_unprocessed_news() -> dict[str, Any]:
    """Finds unprocessed news raw_items, calls LLM to summarize, and saves to processed_items."""
    if not settings.openrouter_api_key:
        log.warning("news_pipeline.openrouter_key_missing")
        return {"status": "error", "reason": "openrouter_key_missing"}

    client = AsyncOpenAI(
        api_key=settings.openrouter_api_key,
        base_url="https://openrouter.ai/api/v1",
    )

    db = SessionLocal()
    try:
        # Find raw items from news sources that don't have a ProcessedItem
        stmt = (
            select(RawItem, Source)
            .join(Source, RawItem.source_id == Source.id)
            .outerjoin(ProcessedItem, ProcessedItem.raw_item_id == RawItem.id)
            .where(Source.type.in_(["rss_global", "twitter", "linkedin_news", "custom_site"]))
            .where(ProcessedItem.id.is_(None))
            .limit(50) # Process in batches to avoid overwhelming LLM/time limits
        )
        result = await db.execute(stmt)
        rows = result.all()

        if not rows:
            log.info("news_pipeline.no_unprocessed_items")
            return {"status": "ok", "processed_count": 0}

        log.info("news_pipeline.processing_batch", count=len(rows))
        
        processed_count = 0
        failed_count = 0

        for raw_item, source in rows:
            text_to_process = raw_item.text or ""
            if not text_to_process:
                # If there's no text, we just mark it as not relevant so it doesn't get picked up again
                pi = ProcessedItem(
                    raw_item_id=raw_item.id,
                    is_relevant=False,
                    relevance_score=0.0,
                    summary="No content",
                    section="news",
                    rank_score=0.0,
                )
                db.add(pi)
                processed_count += 1
                continue

            llm_result = await _process_text_with_llm(text_to_process, client)
            
            if not llm_result:
                failed_count += 1
                continue

            headline = llm_result.get("headline", "").strip()
            summary_text = llm_result.get("summary", "").strip()
            tags = llm_result.get("tags", [])
            
            # Combine into a single summary block for vector embeddings
            summary = f"{headline} | {summary_text}" if headline else summary_text
            if tags and isinstance(tags, list):
                tags_str = ", ".join([str(t) for t in tags])
                summary += f" [Tags: {tags_str}]"
            
            # Catch LLM returning string instead of float for relevance_score
            try:
                relevance = float(llm_result.get("relevance_score", 0.0))
            except ValueError:
                relevance = 0.0
            
            # Map section based on source type
            section = "linkedin" if source.type == "linkedin_news" else "news"

            pi = ProcessedItem(
                raw_item_id=raw_item.id,
                is_relevant=(relevance >= 0.5), # threshold for relevance
                relevance_score=relevance,
                summary=summary,
                section=section,
                rank_score=relevance,
            )
            db.add(pi)
            processed_count += 1
            
            # Avoid rate limits
            await asyncio.sleep(1)

        await db.commit()
        log.info("news_pipeline.batch_complete", processed=processed_count, failed=failed_count)
        return {"status": "ok", "processed_count": processed_count, "failed_count": failed_count}

    except Exception as exc:
        log.error("news_pipeline.error", error=str(exc))
        await db.rollback()
        return {"status": "error", "error": str(exc)}
    finally:
        await db.close()
