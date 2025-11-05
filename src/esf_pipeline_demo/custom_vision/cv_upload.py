# src/esf_pipeline/custom_vision/cv_upload.py
"""Upload images to Azure Custom Vision."""

import json
import os
import time
from collections.abc import Iterable
from io import BytesIO
from logging import getLogger

import pandas as pd
import requests
from azure.cognitiveservices.vision.customvision.training import (
    CustomVisionTrainingClient,
)
from azure.cognitiveservices.vision.customvision.training.models import (
    ImageFileCreateBatch,
    ImageFileCreateEntry,
    Region,
)
from dotenv import load_dotenv
from msrest.authentication import ApiKeyCredentials
from msrest.exceptions import HttpOperationError
from PIL import Image

from ..config.config import LOCAL_PROCESSED_DIR

load_dotenv()

logger = getLogger(__name__)

ENDPOINT = os.getenv("AZURE_CUSTOM_VISION_ENDPOINT", "")
TRAINING_KEY = os.getenv("AZURE_CUSTOM_VISION_TRAINING_KEY", "")
PROJECT_ID = os.getenv("AZURE_CUSTOM_VISION_PROJECT_ID", "")

TAGS_CSV = LOCAL_PROCESSED_DIR / "azure_cv/customvision_tags.csv"
ENTRIES_JSON = LOCAL_PROCESSED_DIR / "azure_cv/customvision_image_entries.json"

BATCH_SIZE = 64
MAX_RETRIES = 6
SLEEP_BETWEEN_BATCHES = 0.2
TAG_CREATION_MAX_RPS = 5.0


def download_and_convert_image(url: str) -> bytes | None:
    """Download image from URL and convert to RGB JPEG format."""
    try:
        # Download image
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Open and check format
        img = Image.open(BytesIO(response.content))

        # Convert to RGB if needed
        if img.mode != "RGB":
            logger.debug(f"Converting image from {img.mode} to RGB: {url}")
            img = img.convert("RGB")

        # Save to bytes as JPEG
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format="JPEG", quality=95)
        return img_byte_arr.getvalue()

    except Exception as e:
        logger.error(f"Failed to download/convert {url}: {e}")
        return None


def upload_to_custom_vision():
    """Upload images to Azure Custom Vision."""
    logger.info("Starting upload to Azure Custom Vision...")
    creds = ApiKeyCredentials(in_headers={"Training-key": TRAINING_KEY})
    trainer = CustomVisionTrainingClient(ENDPOINT, creds)

    name_to_id = _ensure_tags(
        trainer,
        PROJECT_ID,
        TAGS_CSV,
        max_rps=TAG_CREATION_MAX_RPS,
        max_retries=MAX_RETRIES,
        base_sleep=0.5,
    )

    entries = _load_entries(ENTRIES_JSON)
    entries = _map_regions_to_tag_ids(entries, name_to_id)

    _, failures = _upload_batches(
        trainer,
        PROJECT_ID,
        entries,
        batch_size=BATCH_SIZE,
        max_retries=MAX_RETRIES,
        base_sleep=0.5,
        sleep_between_batches=SLEEP_BETWEEN_BATCHES,
    )
    logger.error(f"Failed to upload {len(failures)} entries.")

    for failure in failures:
        logger.error(failure)

    return len(failures), failures[:10]


def _upload_one_batch(
    trainer: CustomVisionTrainingClient,
    project_id: str,
    batch: list[dict],
):
    """Upload one batch of images with automatic format conversion."""
    payload = []

    for e in batch:
        url = e["url"]

        # Download and convert image
        image_data = download_and_convert_image(url)
        if image_data is None:
            logger.error(f"Skipping image due to download/conversion failure: {url}")
            continue

        # Prepare regions
        regions = [
            Region(
                tag_id=r["tag_id"],
                left=r["left"],
                top=r["top"],
                width=r["width"],
                height=r["height"],
            )
            for r in e.get("regions", [])
        ]

        # Use ImageFileCreateEntry instead of ImageUrlCreateEntry
        payload.append(
            ImageFileCreateEntry(
                name=os.path.basename(url), contents=image_data, regions=regions
            )
        )

    if not payload:
        logger.warning("No valid images in batch to upload")
        return None

    # Use create_images_from_files instead of create_images_from_urls
    batch_obj = ImageFileCreateBatch(images=payload)
    return trainer.create_images_from_files(project_id, batch_obj)


def _upload_batches(
    trainer: CustomVisionTrainingClient,
    project_id: str,
    entries: list[dict],
    batch_size: int = 64,
    max_retries: int = 6,
    base_sleep: float = 0.5,
    sleep_between_batches: float = 0.0,
) -> tuple[int, list[tuple[str, str]]]:
    logger.info(f"Uploading {len(entries)} images in batches of {batch_size}...")
    total = len(entries)
    success = 0
    failures: list[tuple[str, str]] = []

    for batch_idx, batch in enumerate(_chunked(entries, batch_size), start=1):
        attempt, sleep = 0, base_sleep
        while True:
            attempt += 1
            try:
                result = _upload_one_batch(trainer, project_id, batch)

                if result is None:
                    # Entire batch failed during download/conversion
                    failures.extend((e["url"], "download_failed") for e in batch)
                    logger.warning(f"[upload] Batch {batch_idx}: entire batch failed")
                    break

                ok = sum(1 for img in result.images if img.status == "OK")
                fail = [
                    (img.source_url or "unknown", img.status)
                    for img in result.images
                    if img.status != "OK"
                ]
                success += ok
                failures.extend(fail)
                logger.info(f"[upload] Batch {batch_idx}: OK={ok}, FAIL={len(fail)}")
                break
            except HttpOperationError as ex:
                status = getattr(ex.response, "status_code", None)
                retry_after = None
                try:
                    retry_after = float(ex.response.headers.get("Retry-After"))
                except Exception:
                    pass
                throttled_statuses = {429, 503}
                if status in throttled_statuses and attempt <= max_retries:
                    wait = retry_after if retry_after else sleep
                    logger.warning(f"[429] Throttled uploading batch {batch_idx}.")
                    logger.warning(
                        f"Backing off {wait:.1f}s (attempt {attempt}/{max_retries})"
                    )
                    time.sleep(wait)
                    sleep = min(sleep * 2, 8.0)
                    continue
                else:
                    logger.error(f"Batch {batch_idx} failed: {ex}")
                    failures.extend((e["url"], "exception") for e in batch)
                    break

        if sleep_between_batches > 0:
            time.sleep(sleep_between_batches)

    logger.info(f"[summary] Uploaded {success}/{total} images successfully.")
    logger.info(f"[summary] {len(failures)} failures.")
    return success, failures


def _respect_rps(next_allowed: float, max_rps: float) -> float:
    now = time.time()
    min_interval = 1.0 / max_rps if max_rps > 0 else 0.0
    if now < next_allowed:
        time.sleep(next_allowed - now)
    return time.time() + min_interval


def _ensure_tags(
    trainer: CustomVisionTrainingClient,
    project_id: str,
    csv_path: str,
    max_rps: float = 5.0,
    max_retries: int = 6,
    base_sleep: float = 0.5,
) -> dict[str, str]:
    df = pd.read_csv(csv_path)
    if not {"id", "name"}.issubset(df.columns):
        raise ValueError("Tags CSV must have columns: id,name")

    existing = {t.name: t for t in trainer.get_tags(project_id)}
    name_to_id: dict[str, str] = {t.name: t.id for t in existing.values()}

    next_allowed = 0.0
    for _, row in df.iterrows():
        tag_name = str(row["name"]).strip()
        if not tag_name or tag_name in name_to_id:
            continue

        next_allowed = _respect_rps(next_allowed, max_rps)

        attempt, sleep = 0, base_sleep
        while True:
            attempt += 1
            try:
                created = trainer.create_tag(project_id, tag_name)
                name_to_id[tag_name] = created.id
                logger.info(f"[tags] Created: {tag_name} ({created.id})")
                break
            except HttpOperationError as ex:
                status = getattr(ex.response, "status_code", None)
                retry_after = None
                try:
                    retry_after = float(ex.response.headers.get("Retry-After"))
                except Exception:
                    pass
                throttled_statuses = {429, 503}
                if status in throttled_statuses and attempt <= max_retries:
                    wait = retry_after if retry_after else sleep
                    logger.warning(
                        f"[429] Throttled creating '{tag_name}'. "
                        f"Backing off {wait:.1f}s (attempt {attempt}/{max_retries})"
                    )
                    time.sleep(wait)
                    sleep = min(sleep * 2, 8.0)
                    continue
                else:
                    raise

    logger.info(f"[tags] Total tags available now: {len(name_to_id)}")
    return name_to_id


def _load_entries(entries_json_path: str) -> list[dict]:
    with open(entries_json_path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "images" in data:
        return data["images"]
    elif isinstance(data, list):
        return data
    else:
        raise ValueError(
            "Entries JSON must be a list of images/objects with an 'images' key."
        )


def _map_regions_to_tag_ids(
    entries: list[dict], name_to_id: dict[str, str]
) -> list[dict]:
    updated = []
    missing = set()

    for e in entries:
        url = e.get("url") or e.get("name")
        if not url:
            continue

        out_regions = []
        for r in e.get("regions", []):
            tid = r.get("tagId")
            if not tid:
                tn = r.get("tagName")
                if not tn or tn not in name_to_id:
                    missing.add(tn)
                    continue
                tid = name_to_id[tn]

            out_regions.append(
                {
                    "tag_id": tid,
                    "left": float(r["left"]),
                    "top": float(r["top"]),
                    "width": float(r["width"]),
                    "height": float(r["height"]),
                }
            )

        updated.append({"url": url, "regions": out_regions})

    if missing:
        logger.warning(
            "These tag names were not found and were skipped: "
            f"{sorted(t for t in missing if t)}"
        )
    return updated


def _chunked(lst: list, n: int) -> Iterable[list]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
