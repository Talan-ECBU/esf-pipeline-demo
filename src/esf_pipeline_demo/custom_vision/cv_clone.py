# src/esf_pipeline/custom_vision/cv_clone.py
"""Clone an Azure Custom Vision project to a new project."""
import functools
import logging
import os
import random
import time

from azure.cognitiveservices.vision.customvision.training import (
    CustomVisionTrainingClient,
)
from azure.cognitiveservices.vision.customvision.training.models import (
    ImageUrlCreateBatch,
    ImageUrlCreateEntry,
    Region,
)
from dotenv import load_dotenv
from msrest.authentication import ApiKeyCredentials

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cv-clone")

ENDPOINT = os.getenv("AZURE_CUSTOM_VISION_ENDPOINT", "")
TRAINING_KEY = os.getenv("AZURE_CUSTOM_VISION_TRAINING_KEY")
SOURCE_PROJECT_ID = os.getenv("AZURE_CUSTOM_VISION_PROJECT_ID")

credentials = ApiKeyCredentials(in_headers={"Training-key": TRAINING_KEY})
trainer = CustomVisionTrainingClient(ENDPOINT, credentials)


def retry_with_backoff(max_attempts=5, base_delay=1.0):
    """Retry decorator for rate-limited operations."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "Too Many Requests" in str(e) or "Service Unavailable" in str(e):
                        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(
                            0, 0.5
                        )
                        logger.warning(
                            f"[{func.__name__}] Rate limit hit, retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                    else:
                        raise
            raise RuntimeError(f"[{func.__name__}] Max retries exceeded.")

        return wrapper

    return decorator


@retry_with_backoff()
def safe_create_tag(project_id: str, name: str):
    return trainer.create_tag(project_id, name)


@retry_with_backoff()
def safe_upload_images(project_id: str, entries: list[ImageUrlCreateEntry]):
    batch_obj = ImageUrlCreateBatch(images=entries)
    return trainer.create_images_from_urls(project_id, batch_obj)


def clone_project(
    source_id: str | None = None, new_project_name: str | None = None
) -> str:
    """Clone an existing Custom Vision project to a new project."""
    source_id = source_id or SOURCE_PROJECT_ID
    new_project_name = new_project_name or "ESF ML Cloned"
    logger.info(f"Cloning project: {source_id}")
    source_project = trainer.get_project(source_id)
    domain = source_project.settings.domain_id
    classification = source_project.settings.classification_type
    logger.info(f"Source project domain: {domain}, type: {classification}")

    new_project = trainer.create_project(
        name=new_project_name, domain_id=domain, classification_type=classification
    )
    logger.info(f"Created new project: {new_project.name} ({new_project.id})")

    source_tags = trainer.get_tags(source_id)
    tag_map = {}
    for tag in source_tags:
        new_tag = safe_create_tag(new_project.id, tag.name)
        tag_map[tag.id] = new_tag
    logger.info(f"Copied {len(tag_map)} tags.")
    images = []

    offset = 0
    while True:
        batch = trainer.get_images(source_id, take=256, skip=offset)
        if not batch:
            break
        images.extend(batch)
        offset += len(batch)
        logger.info(f"Fetched {offset} images...")

    logger.info(f"Total images to copy: {len(images)}")

    for i in range(0, len(images), 64):
        batch = images[i : i + 64]
        entries: list[ImageUrlCreateEntry] = []

        for img in batch:
            if not getattr(img, "original_image_uri", None):
                logger.warning(
                    f"Skipped image {getattr(img, 'id', '?')} (no original_image_uri)"
                )
                continue

            regions_payload: list[Region] = []
            for r in getattr(img, "regions", None) or []:
                new_tag = tag_map.get(r.tag_id)
                if new_tag:
                    regions_payload.append(
                        Region(
                            tag_id=new_tag.id,
                            left=r.left,
                            top=r.top,
                            width=r.width,
                            height=r.height,
                        )
                    )
                else:
                    logger.warning(f"Skipped region tag_id {r.tag_id} (not in tag_map)")

            if regions_payload:
                entries.append(
                    ImageUrlCreateEntry(
                        url=img.original_image_uri,
                        regions=regions_payload,
                    )
                )
            else:
                old_tag_ids = [t.tag_id for t in (getattr(img, "tags", None) or [])]
                new_tag_ids = [tag_map[tid].id for tid in old_tag_ids if tid in tag_map]
                entries.append(
                    ImageUrlCreateEntry(
                        url=img.original_image_uri,
                        tag_ids=new_tag_ids,
                    )
                )

        if entries:
            safe_upload_images(new_project.id, entries)

        logger.info(f"Uploaded {i + len(batch)} / {len(images)} images...")

    logger.info(f"Project cloned to: {new_project.id}")
    return new_project.id
