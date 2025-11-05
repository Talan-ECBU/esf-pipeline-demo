# src/esf_storage/blob_client.py

import logging
from pathlib import Path

from azure.storage.blob import BlobServiceClient

from ..config.config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, setup_logging

setup_logging()

logger = logging.getLogger(__name__)

_CONNECTION_STR = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={AZURE_STORAGE_ACCOUNT};"
    f"AccountKey={AZURE_STORAGE_KEY};"
    f"EndpointSuffix=core.windows.net"
)
_blob_service_client = BlobServiceClient.from_connection_string(_CONNECTION_STR)


def upload_to_blob(local_path: str, container_name: str, blob_path: str) -> None:
    """
    Uploads a local file to specified container / blob path in Azure Blob Storage.
    """
    try:
        logger.info(
            "Starting upload",
            extra={
                "local_path": local_path,
                "container": container_name,
                "blob": blob_path,
            },
        )

        blob_client = _blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )

        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logger.info(
            "Upload successful",
            extra={
                "local_path": local_path,
                "container": container_name,
                "blob": blob_path,
            },
        )

    except FileNotFoundError:
        logger.exception("Local file not found", extra={"local_path": local_path})
        raise

    except Exception:
        logger.exception(
            "Unexpected error during upload",
            extra={
                "local_path": local_path,
                "container": container_name,
                "blob": blob_path,
            },
        )
        raise


def download_blob_to_local(
    container_name: str, blob_path: str, local_path: str
) -> None:
    """
    Downloads a blob from Azure Storage to local file.
    """
    try:
        logger.info(
            "Starting download",
            extra={
                "container": container_name,
                "blob": blob_path,
                "local_path": local_path,
            },
        )

        blob_client = _blob_service_client.get_blob_client(
            container=container_name, blob=blob_path
        )

        # Ensure target directory exists
        target_dir = Path(local_path).parent
        target_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(
            "Ensured local directory exists", extra={"directory": str(target_dir)}
        )

        with open(local_path, "wb") as file:
            stream = blob_client.download_blob()
            file.write(stream.readall())

        logger.info(
            "Download successful",
            extra={
                "container": container_name,
                "blob": blob_path,
                "local_path": local_path,
            },
        )

    except Exception:
        logger.exception(
            "Unexpected error during download",
            extra={
                "container": container_name,
                "blob": blob_path,
                "local_path": local_path,
            },
        )
        raise
