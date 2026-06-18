"""
Orchestrator for generating One Piece TCG tierlist Instagram posts.

Public-facing interface that coordinates the complete workflow:
1. Fetch card grading/market data
2. Generate or fetch tierlist from MongoDB
3. Render tierlist image using private_providers
4. Upload to GCS

Font and image resources are located in private_providers/ for unified management.
"""

import os
import sys
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from dotenv import load_dotenv

from private_providers.instagram.generateTierlist import format_month_year

# Add private_providers to path so we can import generateTierlist
ROOT_DIR = Path(__file__).resolve().parents[2]
PRIVATE_PROVIDERS_PATH = ROOT_DIR / "private_providers" / "instagram"
sys.path.insert(0, str(PRIVATE_PROVIDERS_PATH))

import generateTierlist as tierlist_gen


def generate_tierlist_post(mongo_uri: str, db_name: str, language: str = "en") -> dict[str, Any]:
    print(f"[Orchestrator] Starting tierlist generation workflow")
    print(f"[Orchestrator] MongoDB URI: {mongo_uri}")
    print(f"[Orchestrator] Database: {db_name}")
    print(f"[Orchestrator] Language: {language}")

    # Step 1: Fetch latest tierlist from MongoDB
    print(f"[Orchestrator] Fetching latest tierlist from MongoDB...")
    try:
        tierlist_doc = tierlist_gen.fetch_latest_tierlist(mongo_uri, db_name, language=language)
        print(f"[Orchestrator] ✓ Tierlist fetched: {tierlist_doc.get('_id')}")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to fetch tierlist: {e}")
        raise

    # Step 2: Render tierlist image in memory
    print(f"[Orchestrator] Rendering tierlist image...")
    try:
        image_bytes = tierlist_gen.render_tierlist_post(tierlist_doc)
        print(f"[Orchestrator] ✓ Image rendered: {len(image_bytes)} bytes")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to render image: {e}")
        raise

    # Step 3: Generate caption
    print(f"[Orchestrator] Generating caption...")
    try:
        caption = tierlist_gen.build_caption(tierlist_doc)
        print(f"[Orchestrator] ✓ Caption generated ({len(caption)} chars)")
    except Exception as e:
        print(f"[Orchestrator] ✗ Failed to generate caption: {e}")
        raise

    month_year = format_month_year(tierlist_doc.get("date"))

    # Step 4: Build manifest payload
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

    # Step 5: Upload images to GCS
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

    # Step 6: Save manifest and post document to MongoDB
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
    # Load environment variables from .env files (in order of precedence)
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()
    
    # Example usage
    mongo_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")

    if not mongo_uri or not db_name:
        print("Error: MONGODB_URI and MONGODB_DB environment variables required")
        sys.exit(1)

    try:
        result = generate_tierlist_post(mongo_uri, db_name)
        print("\nFinal Result:")
        print(f"  Layout: {result['layout_version']}")
        print(f"  GCS Bucket: {result['gcs_bucket']}")
        print(f"  Uploaded Images: {len(result['storage_images'])}")
        print(f"  MongoDB ID: {result['mongodb']['insertedId']}")
    except Exception as e:
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
