"""Deprecated — changedetection.io has been removed from the jobs pipeline.

All LLM-based extraction logic now resides in `app.services.jobs.extraction_utils`.
This module provides backwards-compatible imports for any legacy references.
"""
from app.services.jobs.extraction_utils import (  # noqa: F401
    ExtractionError,
    extract_jobs_from_diff,
)
