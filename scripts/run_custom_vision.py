# scripts/run_custom_vision.py
"""Script to control custom vision model."""

import argparse

import pandas as pd

from esf_pipeline.config import LOCAL_PROCESSED_DIR, LOCAL_RAW_DIR, setup_logging
from esf_pipeline.custom_vision.cv_clone import clone_project
from esf_pipeline.custom_vision.cv_predict import get_image_predictions

setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Run Custom Vision tasks.")
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--clone", action="store_true", help="Clone the Custom Vision project."
    )
    group.add_argument(
        "--predict", action="store_true", help="Run predictions on images."
    )

    parser.add_argument(
        "--name", nargs="?", help="Name for the (new) project or model."
    )
    parser.add_argument(
        "--workers", type=int, default=3, help="Number of worker threads."
    )
    parser.add_argument(
        "--max-images",
        type=int,
        default=None,
        help="Maximum number of images to process.",
    )
    parser.add_argument(
        "--root-dir",
        type=str,
        help="Root directory for images relative to data/raw.",
    )

    args = parser.parse_args()

    if args.clone:
        clone_project(new_project_name=args.name)
    elif args.predict:
        images = get_image_predictions(
            root_directory=LOCAL_RAW_DIR / args.root_dir,
            max_workers=args.workers,
            max_images=args.max_images,
        )
        image_df = pd.DataFrame(images)

        image_df.to_csv(
            LOCAL_PROCESSED_DIR / "image_predictions_10_22.csv", index=False
        )


if __name__ == "__main__":
    main()
