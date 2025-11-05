# src/esf_pipeline/custom_vision/cv_predict.py
"""Provide image predictions with Azure Custom Vision."""

__all__ = ["get_image_predictions", "get_single_image_prediction"]

import io
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger

from azure.cognitiveservices.vision.customvision.prediction import (
    CustomVisionPredictionClient,
)
from dotenv import load_dotenv
from msrest.authentication import ApiKeyCredentials
from PIL import Image

load_dotenv()

logger = getLogger(__name__)

ENDPOINT = os.getenv("AZURE_CUSTOM_VISION_ENDPOINT", "")
PREDICTION_KEY = os.getenv("AZURE_CUSTOM_VISION_PREDICTION_KEY", "")
PROJECT_ID = os.getenv("AZURE_CUSTOM_VISION_PROJECT_ID", "")
IMAGE_MODEL_NAME = os.getenv("IMAGE_MODEL_NAME", "Final Image Model")

prediction_credentials = ApiKeyCredentials(
    in_headers={"Prediction-key": PREDICTION_KEY}
)
predictor = CustomVisionPredictionClient(ENDPOINT, prediction_credentials)


def get_image_predictions(
    root_directory: str,
    marketplace: str | None = None,
    max_workers: int = 5,
    max_images: int | None = None,
) -> list[dict]:
    """Get predictions for a batch of images."""
    image_paths = []
    images_added = {}
    for root, _, files in os.walk(root_directory):
        if (not marketplace) or (marketplace in root):
            logger.debug(f"Processing marketplace folder: {root}")
            for file in files:
                if file.endswith(".jpg") and "_image" in file.lower():
                    image_path = os.path.join(root, file)
                    id = _extract_id_from_filepath(image_path)
                    images_added[id] = images_added.get(id, 0) + 1
                    if max_images and images_added[id] > max_images:
                        continue
                    image_paths.append(image_path)

    data = []

    # Single-threaded processing
    if max_workers == 1:
        for image_path in image_paths:
            try:
                result = _process_single_image(image_path)
                if result:
                    data.append(result)
                    logger.info(f"Completed prediction for {image_path}")
            except Exception as e:
                logger.error(f"Exception for {image_path}: {e}")
        logger.info(f"Successfully processed {len(data)} images")
        return data

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {
            executor.submit(_process_single_image, path): path for path in image_paths
        }
        for future in as_completed(future_to_path):
            image_path = future_to_path[future]
            try:
                result = future.result()
                if result:
                    data.append(result)
                    logger.info(f"Completed prediction for {image_path}")
            except Exception as e:
                logger.error(f"Exception for {image_path}: {e}")

    logger.info(f"Successfully processed {len(data)} images")

    return data


def get_single_image_prediction(image_path: str):
    """Get prediction for a single image."""
    logger.info(f"Getting prediction for image: {image_path}")

    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Image file not found: {image_path}")

    file_size = os.path.getsize(image_path)
    if file_size == 0:
        raise ValueError(f"Image file is empty: {image_path}")

    logger.info(f"Image file size: {file_size} bytes")

    try:
        image_data = _convert_image_if_needed(image_path)
        logger.info(f"Sending {len(image_data)} bytes to Custom Vision")

        results = predictor.detect_image(PROJECT_ID, IMAGE_MODEL_NAME, image_data)
        return results
    except Exception as e:
        logger.error(f"Prediction failed for {image_path}: {e!s}")
        raise


def _convert_image_if_needed(image_path: str):
    """Convert image to RGB if needed and return bytes."""
    with Image.open(image_path) as img:
        converted_img = None
        # Convert CMYK or other modes to RGB
        if img.mode != "RGB":
            logger.info(f"Converting image from {img.mode} to RGB")
            converted_img = img.convert("RGB")
        img_byte_arr = io.BytesIO()
        image = converted_img if converted_img else img
        image.save(img_byte_arr, format="JPEG", quality=95)
        img_byte_arr.seek(0)

        return img_byte_arr.read()


def _process_single_image(image_path: str, max_retries: int = 3) -> dict | None:
    """Process a single image with retry logic for rate limiting."""
    for attempt in range(max_retries):
        try:
            results = get_single_image_prediction(image_path)
            result_dict = results.as_dict()
            predictions = result_dict.get("predictions", [])
            if predictions:
                predictions = [
                    {entry["tag_name"]: entry["probability"]} for entry in predictions
                ]
                predictions = sorted(
                    predictions, key=lambda x: next(iter(x.values())), reverse=True
                )
            id = _extract_id_from_filepath(image_path)
            return {"product_id": id, "predictions": predictions}

        except Exception as e:
            error_str = str(e)

            if "Too Many Requests" in error_str or "429" in error_str:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    wait_time = (2**attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"Rate limited for {image_path}. "
                        f"Retrying in {wait_time:.2f}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries exceeded for {image_path}: {e}")
                    return None
            else:
                logger.error(f"Prediction failed for {image_path}: {e}")
                return None

    return None


def _extract_id_from_filepath(filepath):
    """Extract the ID from image filename."""
    filename = os.path.basename(filepath)
    id = filename.split("_image")[0]
    return id
