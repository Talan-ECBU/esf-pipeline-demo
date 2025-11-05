# scripts/rename_blob.py

import argparse
import logging
import sys

from azure.storage.blob import AzureError, BlobServiceClient

from esf_pipeline.config.config import (
    AZURE_STORAGE_ACCOUNT,
    AZURE_STORAGE_KEY,
    setup_logging,
)

setup_logging()

logger = logging.getLogger(__name__)

_CONNECTION_STR = (
    "DefaultEndpointsProtocol=https;"
    f"AccountName={AZURE_STORAGE_ACCOUNT};"
    f"AccountKey={AZURE_STORAGE_KEY};"
    "EndpointSuffix=core.windows.net"
)


def rename_blob_folder(
    container_name: str,
    old_prefix: str,
    new_prefix: str,
    dry_run: bool = False,
    delete_source: bool = True,
) -> None:
    """
    Simulates renaming a 'folder' (prefix) in Azure Blob Storage by
    copying blobs to new names and optionally deleting originals.
    """
    try:
        logger.info(
            "Starting folder rename",
            extra={
                "container": container_name,
                "old_prefix": old_prefix,
                "new_prefix": new_prefix,
                "dry_run": dry_run,
                "delete_source": delete_source,
            },
        )

        blob_service = BlobServiceClient.from_connection_string(_CONNECTION_STR)
        container_client = blob_service.get_container_client(container_name)

        blobs = container_client.list_blobs(name_starts_with=old_prefix)
        count = 0

        for blob in blobs:
            old_blob_name = blob.name
            new_blob_name = new_prefix + old_blob_name[len(old_prefix) :]
            logger.debug(
                "Preparing to rename blob",
                extra={"old_blob": old_blob_name, "new_blob": new_blob_name},
            )

            if dry_run:
                logger.info(
                    "Dry run: would rename blob",
                    extra={"old_blob": old_blob_name, "new_blob": new_blob_name},
                )
            else:
                try:
                    source_url = f"{container_client.url}/{old_blob_name}"
                    new_blob_client = container_client.get_blob_client(new_blob_name)
                    new_blob_client.start_copy_from_url(source_url)
                    logger.info(
                        "Copy initiated",
                        extra={"source_url": source_url, "target_blob": new_blob_name},
                    )

                    if delete_source:
                        old_blob_client = container_client.get_blob_client(
                            old_blob_name
                        )
                        old_blob_client.delete_blob()
                        logger.info(
                            "Deleted source blob", extra={"deleted_blob": old_blob_name}
                        )

                except AzureError:
                    logger.exception(
                        "Azure error during blob rename",
                        extra={"old_blob": old_blob_name, "new_blob": new_blob_name},
                    )
                except Exception:
                    logger.exception(
                        "Unexpected error during blob rename",
                        extra={"old_blob": old_blob_name, "new_blob": new_blob_name},
                    )

            count += 1

        logger.info("Folder rename complete", extra={"blobs_processed": count})

    except AzureError:
        logger.exception("Failed to connect to Azure Blob Storage")
        sys.exit(1)
    except Exception:
        logger.exception("Unexpected failure in rename_blob_folder")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Rename a virtual folder (prefix) in Azure Blob Storage."
    )
    parser.add_argument("--container", required=True, help="Container name")
    parser.add_argument(
        "--old",
        required=True,
        dest="old_prefix",
        help="Old folder prefix (e.g., 'folder1/')",
    )
    parser.add_argument(
        "--new",
        required=True,
        dest="new_prefix",
        help="New folder prefix (e.g., 'folder2/')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only, do not copy or delete blobs",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep original blobs after copying (default is to delete)",
    )

    args = parser.parse_args()

    try:
        rename_blob_folder(
            container_name=args.container,
            old_prefix=args.old_prefix,
            new_prefix=args.new_prefix,
            dry_run=args.dry_run,
            delete_source=not args.keep,
        )
    except Exception:
        logger.exception("Script failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
