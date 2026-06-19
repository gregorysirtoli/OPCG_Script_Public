import os
import sys
import importlib
import traceback
import time
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from dotenv import load_dotenv
from src.core.emailer import send_email
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def load_provider_module(module_name: str):
    module = importlib.import_module(module_name)
    required = [
        "fetch_latest_tierlist",
        "render_tierlist_post",
        "build_caption",
        "upload_images_to_gcs",
        "save_post_manifest_to_mongodb",
        "format_month_year",
        "LAYOUT_VERSION",
        "MONGODB_TIERLIST_COLLECTION",
        "MAIN_TITLE",
    ]
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise AttributeError(f"Module {module_name!r} missing required attributes: {', '.join(missing)}")
    return module


def generate_tierlist_post(mongo_uri: str, db_name: str, language: str = "en") -> dict[str, Any]:
    from private_providers import bundle as providers_bundle

    module_name = getattr(
        providers_bundle,
        "TIERLIST_MODULE",
        "private_providers.instagram.generateTierlist",
    )
    tierlist_gen = load_provider_module(module_name)

    print(f"[Orchestrator] Starting tierlist generation workflow")
    print(f"[Orchestrator] MongoDB URI: {mongo_uri}")
    print(f"[Orchestrator] Database: {db_name}")
    print(f"[Orchestrator] Language: {language}")
    print(f"[Orchestrator] Provider module: {module_name}")

    print(f"[Orchestrator] Fetching latest tierlist from MongoDB...")
    try:
        tierlist_doc = tierlist_gen.fetch_latest_tierlist(mongo_uri, db_name, language=language)
        print(f"[Orchestrator] ✓ Tierlist fetched: {tierlist_doc.get('_id')}")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to fetch tierlist: {e}")
        raise

    print(f"[Orchestrator] Rendering tierlist image...")
    try:
        image_bytes = tierlist_gen.render_tierlist_post(tierlist_doc)
        print(f"[Orchestrator] ✓ Image rendered: {len(image_bytes)} bytes")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to render image: {e}")
        raise

    print(f"[Orchestrator] Generating caption...")
    try:
        caption = tierlist_gen.build_caption(tierlist_doc)
        print(f"[Orchestrator] ✓ Caption generated ({len(caption)} chars)")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to generate caption: {e}")
        raise

    month_year = tierlist_gen.format_month_year(tierlist_doc.get("date"))

    print(f"[Orchestrator] Building manifest payload...")
    content = {
        "title": f"Tierlist - {month_year}",
        "hook": tierlist_gen.MAIN_TITLE,
        "caption": caption,
        "hashtags": [
            "#OnePiece",
            "#Luffy",
            "#OnePieceCardGame",
            "#OnePieceAnime",
            "#OnePieceTCG",
            "#OnePieceFans",
            "#OPTCG",
            "#OPCG",
            "#OPCardGame",
        ],
        "tiers": tierlist_doc.get("tiers", {}),
    }

    manifest_payload: dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "layout_version": tierlist_gen.LAYOUT_VERSION,
        "source": {
            "collection": tierlist_gen.MONGODB_TIERLIST_COLLECTION,
            "tierlist_id": str(tierlist_doc.get("_id")),
            "language": tierlist_doc.get("language"),
            "date": tierlist_doc.get("date"),
        },
        "content": content,
        "storage": {
            "provider": "google_cloud_storage",
            "bucket": None,
            "prefix": None,
            "images": [],
        },
        "post_state": {
            "status": os.getenv("INSTAGRAM_POST_INITIAL_STATUS", "draft").strip() or "draft",
            "scheduledAt": os.getenv("INSTAGRAM_POST_SCHEDULED_AT") or None,
            "publishedAt": os.getenv("INSTAGRAM_POST_PUBLISHED_AT") or None,
            "lockedAt": os.getenv("INSTAGRAM_POST_LOCKED_AT") or None,
            "lockedBy": os.getenv("INSTAGRAM_POST_LOCKED_BY") or None,
        },
    }
    print(f"[Orchestrator] ✓ Manifest payload built")

    print(f"[Orchestrator] Uploading images to Google Cloud Storage...")
    try:
        storage_payload = tierlist_gen.upload_images_to_gcs([image_bytes], tierlist_doc)
        manifest_payload["storage"] = storage_payload
        print(f"[Orchestrator] ✓ Images uploaded to GCS")
        print(f"[Orchestrator]   Bucket: {storage_payload.get('bucket')}")
        print(f"[Orchestrator]   Prefix: {storage_payload.get('prefix')}")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to upload to GCS: {e}")
        raise

    print(f"[Orchestrator] Saving post manifest to MongoDB...")
    try:
        mongo_payload = tierlist_gen.save_post_manifest_to_mongodb(mongo_uri, db_name, manifest_payload)
        manifest_payload["mongodb"] = mongo_payload
        print(f"[Orchestrator] ✓ Post manifest saved to MongoDB")
        print(f"[Orchestrator]   Document ID: {mongo_payload.get('insertedId')}")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to save to MongoDB: {e}")
        raise

    print(f"[Orchestrator] ✓ Workflow completed successfully")
    return {
        "layout_version": tierlist_gen.LAYOUT_VERSION,
        "gcs_bucket": storage_payload.get("bucket"),
        "gcs_prefix": storage_payload.get("prefix"),
        "storage_images": storage_payload.get("images", []),
        "mongodb": mongo_payload,
    }


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()

    try:
        mongo_uri = os.getenv("MONGODB_URI")
        db_name = os.getenv("MONGODB_DB")

        if not mongo_uri or not db_name:
            raise RuntimeError("MONGODB_URI and MONGODB_DB environment variables required")

        result = generate_tierlist_post(mongo_uri, db_name)
        print("\nFinal Result:")
        print(f"  Layout: {result['layout_version']}")
        print(f"  GCS Bucket: {result['gcs_bucket']}")
        print(f"  Uploaded Images: {len(result['storage_images'])}")
        print(f"  MongoDB ID: {result['mongodb']['insertedId']}")

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = "✅ [1/1][INSTAGRAM] Tierlist generation completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception as e:
        send_email("🚫 [1/1][INSTAGRAM] Tierlist generation failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
