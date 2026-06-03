import asyncio
import logging
from typing import Literal

__all__ = ["check_for_exceptions"]

LOGGER = logging.getLogger(__name__)


def check_for_exceptions(
    task: asyncio.Task,
    log_lvl: Literal["EXCEPTION", "ERROR", "INFO", "DEBUG"],
    caller_class_name: str,
) -> None:
    """Check if an asyncio task has completed with an exception and log it if so.

    :param task: The asyncio.Task to check.
    :param log_lvl: The log level to use for logging exceptions.
    :param caller_class_name: The name of the class that initiated the task.
    """
    # Check for exceptions
    try:
        task.result()  # This will raise if there was an exception
    except asyncio.CancelledError:
        LOGGER.debug("Task %s was cancelled", task.get_name())
    except Exception as e:
        extra = {"class_name": caller_class_name}
        message = "Error in %s task: %s"

        if log_lvl == "EXCEPTION":
            LOGGER.exception(message, task.get_name(), e, extra=extra)
        elif log_lvl == "ERROR":
            LOGGER.error(message, task.get_name(), e, extra=extra)
        elif log_lvl == "INFO":
            LOGGER.info(message, task.get_name(), e, extra=extra)
        elif log_lvl == "DEBUG":
            LOGGER.debug(message, task.get_name(), e, extra=extra)
