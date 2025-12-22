# register_synoptic_websockets.py
import os
import sys
import logging
from typing import Any, Dict, List
from pathlib import Path

import yaml
from dotenv import load_dotenv
from ndp_ep import APIClient
from scidx_streaming import StreamingClient
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("register_synoptic_websockets")


def make_synoptic_url(token: str, stid: str, vars_list: List[str]) -> str:
    vars_str = ",".join(vars_list)
    return f"wss://push.synopticdata.com/feed/{token}/?stid={stid}&vars={vars_str}"


def station_dataset_metadata(cfg: Dict[str, Any], st: Dict[str, Any]) -> Dict[str, Any]:
    syn = cfg.get("synoptic") or {}

    stid = st["station_id"]
    name = st.get("name", stid)
    region = cfg["region"]["name"]

    extras = {
        "dataset_kind": "sensor",
        "region": region,
        "station_id": stid,
        "station_name": name,
        "latitude": st.get("lat"),
        "longitude": st.get("lon"),
        "elevation_m": st.get("elevation_m"),
        "source": "synoptic_push_websocket",
        "synoptic_vars": ",".join(syn.get("vars", []) or []),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "name": f"synoptic_push_{stid.lower()}_aq",
        "title": f"Synoptic Push Air Quality ({stid})",
        "notes": f"Real-time air quality observations from Synoptic Push for station {stid} ({name}).",
        "owner_org": syn.get("owner_org", "synoptic"),
        "extras": extras,
    }


def station_ws_method(synoptic_token: str, st: Dict[str, Any], vars_list: List[str]) -> Dict[str, Any]:
    stid = st["station_id"]
    return {
        "type": "websocket",
        "name": f"synoptic-push-wss-{stid.lower()}",
        "description": f"Synoptic Push WebSocket stream for {stid}.",
        "config": {
            "url": make_synoptic_url(synoptic_token, stid, vars_list),
            "token_env": "SYNOPTIC_TOKEN",
            "station_id": stid,
            "station_name": st.get("name", stid),
            "latitude": st.get("lat"),
            "longitude": st.get("lon"),
            "elevation_m": st.get("elevation_m"),
        },
    }


def register_all_stations(client: APIClient, streaming: StreamingClient, server: str, synoptic_token: str, cfg: Dict[str, Any]) -> None:
    syn = cfg.get("synoptic") or {}
    vars_list = syn.get("vars") or []
    if not vars_list:
        raise RuntimeError("config.yaml missing synoptic.vars")

    stations = cfg.get("stations") or []
    if not stations:
        raise RuntimeError("config.yaml missing stations list")

    for st in stations:
        if "station_id" not in st:
            logger.warning("Skipping station without station_id: %s", st)
            continue

        dataset_metadata = station_dataset_metadata(cfg, st)
        ws_method = station_ws_method(synoptic_token, st, vars_list)
        ds = streaming.register_data_source(
            dataset_metadata=dataset_metadata,
            methods=[ws_method],
            server=server,
        )
        logger.info("Registered dataset=%s (station=%s)", ds.get("name", dataset_metadata["name"]), st["station_id"])


def main():

    here = Path(__file__).resolve()
    project_root = here.parent.parent
    config_path = project_root / "config.yaml"
    cfg = yaml.safe_load(open(config_path))

    load_dotenv(override=True)

    TOKEN = os.getenv("TOKEN")
    API_URL = os.getenv("API_URL")
    SERVER = os.getenv("SERVER", "local")
    SYNOPTIC_TOKEN = os.getenv("SYNOPTIC_TOKEN")

    if not TOKEN or not API_URL or not SYNOPTIC_TOKEN:
        raise RuntimeError("Missing TOKEN, API_URL, or SYNOPTIC_TOKEN in environment.")

    client = APIClient(base_url=API_URL, token=TOKEN)
    streaming = StreamingClient(client)
    logger.info("Streaming Client initialized. User ID: %s", streaming.user_id)

    register_all_stations(client=client, streaming=streaming, server=SERVER, synoptic_token=SYNOPTIC_TOKEN, cfg=cfg)
    logger.info("Done registering Synoptic WebSockets from %s", config_path)


if __name__ == "__main__":
    main()
