import os

import structlog


def configure_logging() -> None:
    renderer: structlog.types.Processor = (
        structlog.dev.ConsoleRenderer()
        if os.getenv("LOG_FORMAT", "pretty") == "pretty"
        else structlog.processors.JSONRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )
