import os
from pathlib import Path
from dotenv import load_dotenv
import yaml
import logging
from ndp_ep import APIClient
from scidx_streaming import StreamingClient
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("register_hotspots_csv")



def main():
    here = Path(__file__).resolve()
    project_root = here.parent.parent
    config_path = project_root / "config.yaml"
    cfg = yaml.safe_load(open(config_path))

    # --- required config ---
    hotspot_url = cfg["hotspot"]["url"]
    region = cfg["region"]["name"]
    owner_org = cfg.get("hotspot", {}).get("owner_org", "hotspot")

    load_dotenv(override=True)

    # --- SciDX credentials ---
    TOKEN = os.environ["TOKEN"]
    API_URL = os.environ["API_URL"]
    SERVER = os.environ.get("SERVER", "local")

    if not TOKEN or not API_URL:
        raise RuntimeError("Missing TOKEN or API_URL in environment")

    client = APIClient(base_url=API_URL, token=TOKEN)
    streaming = StreamingClient(client)
    logger.info("Streaming client initialized. user_id=%s", streaming.user_id)

    # --- dataset metadata ---
    dataset_name = f"hotspots_{region}"

    dataset_metadata = {
        "name": dataset_name,
        "title": f"Forecast Hotspot Events ({region})",
        "notes": (
            "Detected PM2.5 hotspot events generated from forecast-based analysis. "
            "CSV is hosted remotely and consumed via HTTP."
        ),
        "owner_org": owner_org,
        "extras": {
            "dataset_kind": "hotspot",
            "region": region,
            "source": "forecast",
            "model": "aurora",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    # --- CSV consumption method ---
    method = {
        "type": "csv",
        "name": "hotspots_csv_remote",
        "description": "Remote CSV containing detected hotspot events",
        "config": {
            "url": hotspot_url,
        },
        "processing": {
            "delimiter": ",",
            "header_line": 0,
            "start_line": 1,
        },
    }

    client.delete_resource_by_name(dataset_name, server=SERVER)
    streaming.register_data_source(
        dataset_metadata=dataset_metadata,
        methods=[method],
        server=SERVER,
    )

    logger.info("Registered hotspot CSV dataset '%s' from %s", dataset_name, hotspot_url)


if __name__ == "__main__":
    main()
