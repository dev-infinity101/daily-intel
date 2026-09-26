import asyncio
import time
from typing import Any

import structlog

log = structlog.get_logger()

# Global state for leaky bucket rate limiting
_next_allowed_time = 0.0
_lock = asyncio.Lock()

# Tensormux limits optimized for glm-4-7-flash (Zhipu AI)
RPM_LIMIT = 60.0
TPM_LIMIT = 500000.0


async def call_llm_with_rate_limit(
    client: Any,
    model: str,
    messages: list[dict],
    max_tokens: int = 4096,
    timeout: float = 120.0,
    **kwargs,
) -> Any:
    """
    Paced execution of LLM calls to respect the global RPM and TPM limits.
    """
    global _next_allowed_time

    # Estimate input tokens (assuming ~4 chars per token)
    prompt_chars = sum(len(str(m.get("content", ""))) for m in messages)
    estimated_input_tokens = max(1.0, prompt_chars / 4.0)
    
    # Conservatively estimate the output tokens (most JSON outputs are under 1500 tokens)
    estimated_output_tokens = min(1500, max_tokens)
    total_est_tokens = estimated_input_tokens + estimated_output_tokens

    # Calculate required cooldown (seconds) for this specific request
    # Limit 1: 15 RPM -> 60s / 15 = 4.0s per request minimum
    # Limit 2: 20000 TPM -> 60s / 20000 = 0.003s per token
    cooldown_rpm = 60.0 / RPM_LIMIT
    cooldown_tpm = total_est_tokens * (60.0 / TPM_LIMIT)
    
    # Request time cost is bounded by whichever limit is stricter for this payload
    required_cooldown = max(cooldown_rpm, cooldown_tpm)

    async with _lock:
        now = time.time()
        if _next_allowed_time > now:
            wait_s = _next_allowed_time - now
            log.info(
                "llm_client.rate_limit_wait", 
                wait_seconds=round(wait_s, 2), 
                est_tokens=int(total_est_tokens)
            )
            await asyncio.sleep(wait_s)
            # Advance the global clock by the cost of this request
            _next_allowed_time += required_cooldown
        else:
            # If the pipeline is idle, we can start immediately, but subsequent
            # requests must wait until this request's cooldown finishes
            _next_allowed_time = time.time() + required_cooldown

    # Execute the API call
    return await client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        timeout=timeout,
        **kwargs
    )
