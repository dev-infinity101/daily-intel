"""Module 2 — changedetection.io API client.

The user registers a URL via POST /admin/news/custom-sites. This module
calls the changedetection.io REST API to create a watch, configured to
fire a JSON webhook back to /ingest/news/changedetection when the page
changes. The webhook handler (news_admin router) strips HTML, runs the
keyword filter, then writes to raw_items.

changedetection.io uses Apprise for notifications.
json:// URLs trigger plain HTTP POST with the rendered notification body.
"""
import httpx
import structlog

from app.config import settings

log = structlog.get_logger()

_NOTIFICATION_BODY = (
    '{"watch_url":"{{watch_url}}",'
    '"title":"{{title}}",'
    '"diff":"{{diff}}",'
    '"uuid":"{{uuid}}"}'
)


def _cd_base() -> str:
    return settings.changedetection_url.rstrip("/") + "/api/v1"


def _headers() -> dict[str, str]:
    if settings.changedetection_api_key:
        return {"x-api-key": settings.changedetection_api_key}
    return {}


def _apprise_webhook_url() -> str:
    # changedetection.io runs in Docker; it reaches FastAPI via host.docker.internal.
    host = settings.changedetection_webhook_host
    port = settings.changedetection_webhook_port
    return f"json://{host}:{port}/ingest/news/changedetection"


async def register_watch(url: str, display_name: str) -> dict:
    payload = {
        "url": url,
        "title": display_name,
        "notification_urls": [_apprise_webhook_url()],
        "notification_title": f"[Intel] {display_name}",
        "notification_body": _NOTIFICATION_BODY,
        "time_between_check": {"hours": 2},
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{_cd_base()}/watch", json=payload, headers=_headers())
        r.raise_for_status()
        log.info("custom_site.registered", url=url, display_name=display_name)
        return r.json()


async def list_watches() -> list[dict]:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{_cd_base()}/watch", headers=_headers())
        r.raise_for_status()
        data: dict = r.json()
    if not isinstance(data, dict):
        return []
    return [{"uuid": uuid, **info} for uuid, info in data.items()]


async def delete_watch(uuid: str) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.delete(f"{_cd_base()}/watch/{uuid}", headers=_headers())
        r.raise_for_status()
    log.info("custom_site.deleted", uuid=uuid)
