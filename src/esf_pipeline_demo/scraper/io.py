# src/esf_pipeline/scraper/io.py
"""IO functions for requesting, saving and uploading data."""

import json
import logging
from pathlib import Path

from ..config.config import AZURE_RAW_CONTAINER
from ..storage.blob_client import upload_to_blob

logger = logging.getLogger(__name__)


def save_json(data, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=4)


def save_html(data, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(data)


def save_and_upload_json(
    dict_obj: dict, local_path: str, blob_path: str | None = None
) -> None:
    """
    Saves a JSON object to local file and uploads to Blob Storage.

    If blob_path is None, upload is skipped.

    Parameters
    ----------
    dict_obj : dict
        The JSON-serializable dictionary to save.
    local_path : str
        The local file path to save the JSON.
    blob_path : str | None, optional
        The blob path in Azure Storage to upload the JSON. If None, upload is skipped.

    Raises
    ------
    Exception
        If saving or uploading fails.
    """
    try:
        logger.debug("Saving JSON locally", extra={"path": local_path})
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "w") as f:
            json.dump(dict_obj, f, indent=2)
        logger.info("Local JSON saved", extra={"path": local_path})

        if blob_path is None:
            logger.debug("No blob_path provided, skipping upload")
            return None

        logger.debug(
            "Uploading JSON to blob",
            extra={"container": AZURE_RAW_CONTAINER, "blob": blob_path},
        )
        upload_to_blob(str(local_path), AZURE_RAW_CONTAINER, blob_path)
        logger.info("JSON uploaded to blob", extra={"blob_path": blob_path})

    except Exception:
        logger.exception(
            "Failed to save and upload JSON",
            extra={"path": local_path, "blob": blob_path},
        )
        raise


def save_and_upload_images(
    image: bytes, local_path: str, blob_path: str | None = None
) -> None:
    """
    Saves image bytes to local file and uploads to Blob Storage.

    If blob_path is None, upload is skipped.

    Parameters
    ----------
    image : bytes
        The image data in bytes.
    local_path : str
        The local file path to save the image.
    blob_path : str | None, optional
        The blob path in Azure Storage to upload the image. If None, upload is skipped.
    Raises
    ------
    Exception
        If saving or uploading fails.
    """
    try:
        logger.debug("Saving image locally", extra={"path": local_path})

        Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        with open(local_path, "wb") as file:
            file.write(image)

        logger.info("Local image saved", extra={"path": local_path})

        if blob_path is None:
            logger.debug("No blob_path provided, skipping upload")
            return None

        logger.debug(
            "Uploading image to blob",
            extra={"container": AZURE_RAW_CONTAINER, "blob": blob_path},
        )
        upload_to_blob(str(local_path), AZURE_RAW_CONTAINER, blob_path)
        logger.info("Image uploaded to blob", extra={"blob_path": blob_path})

    except Exception:
        logger.exception(
            "Failed to save and upload image",
            extra={"path": local_path, "blob": blob_path},
        )
        raise
