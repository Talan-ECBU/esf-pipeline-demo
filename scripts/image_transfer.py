# scripts/image_transfer.py
"""
Script to transfer labelled training images from Azure ML labelling studio to
custom vision using the provided COCO JSON file.
"""

import argparse
from logging import getLogger

from esf_pipeline.config.config import setup_logging
from esf_pipeline.custom_vision.coco_to_cv import coco_to_customvision, load_coco
from esf_pipeline.custom_vision.cv_upload import upload_to_custom_vision

setup_logging()

logger = getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coco", required=True, help="Path to COCO JSON")
    args: str = ap.parse_args()

    coco = load_coco(args.coco)
    summary = coco_to_customvision(coco)

    logger.info("COCO processing completed...")
    for k, v in summary.items():
        logger.info(f"{k}: {v}")

    logger.info("Uploading to Azure Custom Vision...")

    num_failures, _ = upload_to_custom_vision()

    if num_failures:
        logger.error(f"Failed to upload {num_failures} entries.")
    else:
        logger.info("All assets uploaded successfully.")


if __name__ == "__main__":
    main()
