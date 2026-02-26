import logging

import uvicorn

from project.utils.logging_formatter import setup_logging
from .app import create_app

logger = logging.getLogger("ch-api-gateway")


def main(config: dict) -> None:
    setup_logging("ch-api-gateway")

    logger.info(
        "action=api_init status=starting host=%s port=%d",
        config["api"]["host"],
        config["api"]["port"]
    )

    app = create_app(config)

    uvicorn.run(
        app,
        host=config["api"]["host"],
        port=config["api"]["port"],
        reload=False
    )
