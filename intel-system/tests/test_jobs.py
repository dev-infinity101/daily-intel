"""Unit tests for job adapters — no live network calls."""
import json
from pathlib import Path

import pytest

from app.services.jobs.classifier import extract_skills, infer_experience_level, infer_remote
from app.services.jobs.normalizer import compute_job_dedup_hash, normalize_to_raw_text
from app.services.jobs.free_apis.remotive import _parse as remotive_parse
from app.services.jobs.free_apis.hn import _parse_comment as hn_parse_comment

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_dedup_hash_is_deterministic() -> None:
    h1 = compute_job_dedup_hash("Amazon", "SDE-2", "Bangalore", "https://amazon.jobs/123?ref=foo")
    h2 = compute_job_dedup_hash("Amazon", "SDE-2", "Bangalore", "https://amazon.jobs/123?ref=bar")
    assert h1 == h2, "Query params should be stripped — same URL canonical"


def test_dedup_hash_differs_on_title() -> None:
    h1 = compute_job_dedup_hash("Amazon", "SDE-2", "Bangalore", "https://amazon.jobs/123")
    h2 = compute_job_dedup_hash("Amazon", "SDE-3", "Bangalore", "https://amazon.jobs/123")
    assert h1 != h2


def test_classify_experience_level() -> None:
    assert infer_experience_level("Senior Software Engineer") == "senior"
    assert infer_experience_level("Lead ML Engineer") == "lead"
    assert infer_experience_level("Junior Python Developer") == "entry"
    assert infer_experience_level("Software Engineer") == "mid"


def test_extract_skills() -> None:
    skills = extract_skills("Senior Python Engineer", "5+ years with FastAPI, Postgres, Redis, Docker")
    assert "python" in skills
    assert "fastapi" in skills
    assert "postgres" in skills


def test_infer_remote_from_location() -> None:
    assert infer_remote("Remote") is True
    assert infer_remote("Bangalore, India") is None


def test_remotive_parse() -> None:
    fixture = json.loads((FIXTURE_DIR / "remotive_response.json").read_text())
    job = remotive_parse(fixture["jobs"][0])
    assert job.company == "Acme Corp"
    assert job.job_title == "Senior Backend Engineer"
    assert job.source_type == "remotive"
    assert job.remote is True


def test_hn_parse_comment() -> None:
    fixture = json.loads((FIXTURE_DIR / "hn_response.json").read_text())
    job = hn_parse_comment(fixture["hits"][0])
    assert job is not None
    assert "Acme AI" in job.company or "ML Engineer" in job.job_title
    assert job.source_type == "hn"
