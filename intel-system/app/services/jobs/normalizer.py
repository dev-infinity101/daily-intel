import hashlib
import re
import unicodedata
from urllib.parse import urlparse, urlunparse

from app.schemas.job import JobIn


def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).lower().strip()
    return re.sub(r"\s+", " ", s)


def _canonicalize_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def compute_job_dedup_hash(company: str, title: str, location: str | None, url: str) -> str:
    key = (
        _normalize(company)
        + "|"
        + _normalize(title)
        + "|"
        + _normalize(location or "")
        + "|"
        + _canonicalize_url(url)
    )
    return hashlib.sha256(key.encode()).hexdigest()


def normalize_to_raw_text(job: JobIn) -> str:
    """Return a plain-text representation suitable for content_hash dedup."""
    return f"{job.company} | {job.job_title} | {job.location or ''} | {job.job_url}"
