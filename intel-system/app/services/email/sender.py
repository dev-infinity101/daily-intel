import structlog

from app.config import settings

log = structlog.get_logger()


async def send_email(subject: str, html: str) -> str | None:
    """Send via Resend. Returns the provider message ID."""
    return await _send_resend(subject, html)


async def _send_resend(subject: str, html: str) -> str | None:
    import resend  # type: ignore[import]

    resend.api_key = settings.resend_api_key
    recipients = [e.strip() for e in settings.email_to.split(",") if e.strip()]
    params: resend.Emails.SendParams = {  # type: ignore[assignment]
        "from": settings.email_from,
        "to": recipients,
        "subject": subject,
        "html": html,
    }
    response = resend.Emails.send(params)
    # resend v2.x returns an Email object; v0.x returned a dict — handle both
    msg_id: str | None = response.id if hasattr(response, "id") else response.get("id")  # type: ignore[union-attr]
    log.info("email.sent.resend", to=recipients, message_id=msg_id)
    return msg_id
