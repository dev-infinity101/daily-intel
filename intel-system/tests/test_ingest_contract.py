import json
from pathlib import Path

import pytest
from httpx import AsyncClient

FIXTURE_DIR = Path(__file__).parent / "fixtures"
HEADERS = {"X-Ingest-Token": "dev-token", "X-Source-Type": "telegram_channel"}


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


@pytest.mark.asyncio
@pytest.mark.integration
async def test_ingest_accepts_items(client: AsyncClient) -> None:
    payload = _load_fixture("sample_ingest.json")
    r = await client.post("/ingest", json=payload, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["accepted"] >= 1
    assert data["duplicates"] == 0


@pytest.mark.asyncio
@pytest.mark.integration
async def test_ingest_deduplicates(client: AsyncClient) -> None:
    payload = _load_fixture("sample_ingest.json")
    # First insert
    await client.post("/ingest", json=payload, headers=HEADERS)
    # Second insert — all should be duplicates
    r = await client.post("/ingest", json=payload, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["accepted"] == 0
    assert data["duplicates"] >= 1


@pytest.mark.asyncio
async def test_ingest_rejects_bad_token(client: AsyncClient) -> None:
    payload = _load_fixture("sample_ingest.json")
    r = await client.post(
        "/ingest",
        json=payload,
        headers={"X-Ingest-Token": "wrong", "X-Source-Type": "telegram_channel"},
    )
    assert r.status_code == 401


@pytest.mark.asyncio
@pytest.mark.integration
async def test_ingest_missing_source_type(client: AsyncClient) -> None:
    payload = _load_fixture("sample_ingest.json")
    r = await client.post("/ingest", json=payload, headers={"X-Ingest-Token": "dev-token"})
    assert r.status_code == 422
