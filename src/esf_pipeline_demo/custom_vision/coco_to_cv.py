# src/esf_pipeline/custom_vision/coco_to_customvision.py
"""Convert COCO format to Custom Vision format."""


import csv
import json

from ..config.config import LOCAL_PROCESSED_DIR


def load_coco(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def coco_to_customvision(coco: dict) -> dict:
    images = {img["id"]: img for img in coco.get("images", [])}
    annotations = coco.get("annotations", [])
    categories = coco.get("categories", [])

    cat_id_to_name = _build_category_maps(categories)

    regions_by_image = {}
    for ann in annotations:
        img_id = ann["image_id"]
        cat_id = ann["category_id"]
        img = images.get(img_id)
        if not img:
            continue

        bbox = ann.get("bbox")
        expected_bbox_len = 4
        if not bbox or len(bbox) != expected_bbox_len:
            continue

        left, top, width, height = _normalize_bbox(
            bbox,
            img_w=img.get("width", 0),
            img_h=img.get("height", 0),
        )

        regions_by_image.setdefault(img_id, []).append(
            {
                "tagName": cat_id_to_name.get(cat_id, f"cat_{cat_id}"),
                "left": left,
                "top": top,
                "width": width,
                "height": height,
            }
        )

    entries = []
    missing_url = 0
    for img_id, img in images.items():
        url = _choose_image_url(img)
        if not url:
            missing_url += 1
            continue
        entries.append({"url": url, "regions": regions_by_image.get(img_id, [])})

    out_json_path = LOCAL_PROCESSED_DIR / "azure_cv/customvision_image_entries.json"
    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump({"images": entries}, f, indent=2)

    out_tags_csv_path = LOCAL_PROCESSED_DIR / "azure_cv/customvision_tags.csv"
    out_tags_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_tags_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name"])
        for c in sorted(categories, key=lambda x: x["id"]):
            writer.writerow([c["id"], c.get("name", f"cat_{c['id']}")])

    return {
        "num_images": len(images),
        "num_annotations": len(annotations),
        "num_categories": len(categories),
        "num_entries_with_url": len(entries),
        "num_images_without_url": missing_url,
    }


def _choose_image_url(img: dict) -> str:
    return img.get("absolute_url") or img.get("coco_url") or img.get("file_name")


def _normalize_bbox(bbox: list, img_w: int, img_h: int) -> tuple:
    x, y, w, h = bbox
    if max(x, y, w, h) > 1.0:
        left = x / img_w if img_w else 0.0
        top = y / img_h if img_h else 0.0
        width = w / img_w if img_w else 0.0
        height = h / img_h if img_h else 0.0
    else:
        left, top, width, height = x, y, w, h

    def clamp01(v):
        return max(0.0, min(1.0, float(v)))

    left = clamp01(left)
    top = clamp01(top)
    width = clamp01(width)
    height = clamp01(height)
    return left, top, width, height


def _build_category_maps(categories):
    id_to_name = {}
    for c in categories:
        id_to_name[c["id"]] = c.get("name", f"cat_{c['id']}")
    return id_to_name
