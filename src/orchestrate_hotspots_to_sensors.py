import os
import math
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

from ndp_ep import APIClient
from scidx_streaming import StreamingClient
from datetime import datetime, timezone


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def load_config():
    here = Path(__file__).resolve()
    project_root = here.parent.parent
    cfg_path = project_root / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _as_extras_dict(ds: dict) -> dict:
    extras = ds.get("extras", {})
    return extras if isinstance(extras, dict) else {}

def parse_iso(dt):
    if not dt:
        return None
    return datetime.fromisoformat(dt.replace("Z", "+00:00"))


def main():
    load_dotenv(override=True)
    cfg = load_config()

    # Expect config like: cfg["region"]["name"] == "utah" (or similar)
    region = str(cfg.get("region", {}).get("name", "")).strip()
    hotspot_url = str(cfg.get("hotspot", {}).get("url", "")).strip()

    if not region:
        raise RuntimeError("Missing cfg['region']['name'] in config.yaml")

    API_URL = os.environ.get("API_URL", "").strip()
    TOKEN = os.environ.get("TOKEN", "").strip()
    SERVER = (os.environ.get("SERVER") or "local").strip()  # <- important default

    if not API_URL or not TOKEN:
        raise RuntimeError("Missing API_URL or TOKEN in environment")

    # ------------------------------------------------------------------
    # Initialize SciDX / Streaming
    # ------------------------------------------------------------------
    client = APIClient(base_url=API_URL, token=TOKEN)
    # streaming = StreamingClient(client)

    # ------------------------------------------------------------------
    # 1) Discover hotspot dataset via SciDX (extras-based search)
    # ------------------------------------------------------------------
    hotspot_ds = client.search_datasets(
        terms=[hotspot_url],
        # terms=["hotspots", region],
        # keys=["extras_dataset_kind", "extras_region"],
        server=SERVER,
    )[0]

    # hotspot_ds = streaming.search_consumption_methods(terms=[hotspot_url])[0]

    if not hotspot_ds:
        raise RuntimeError(
            f"No hotspot dataset found for url='{hotspot_url}'"
        )

    # datasets_with_time = [
    #     d for d in datasets
    #     if isinstance(d.get("extras"), dict) and d["extras"].get("updated_at")
    # ]

    # if not datasets_with_time:
    #     raise RuntimeError("No hotspot dataset has extras.updated_at")

    # datasets_with_time.sort(key=lambda d: parse_iso(d["extras"]["updated_at"]), reverse=True,)


    hotspots = pd.read_csv(hotspot_url)

    required_cols = {"lat", "lon"}
    missing = required_cols - set(hotspots.columns)
    if missing:
        raise RuntimeError(
            f"Hotspot CSV missing required columns: {sorted(missing)}."
            f"Found columns: {list(hotspots.columns)}"
        )

    # ------------------------------------------------------------------
    # 2) Discover sensor datasets via SciDX (extras-based search)
    # ------------------------------------------------------------------
    sensor_datasets = client.search_datasets(
        terms=["sensor", region],
        keys=["extras_dataset_kind", "extras_region"],
        server=SERVER,
    )
    # sensor_datasets = streaming.search_consumption_methods(terms=["sensor", region])
    print(f"Found {len(sensor_datasets)} sensor datasets")
    sensors = []
    for ds in sensor_datasets:
        if not isinstance(ds, dict):
            continue

        extras = _as_extras_dict(ds)

        # Try common fields
        lat = extras.get("latitude")
        lon = extras.get("longitude")
        if lat is None or lon is None:
            print(f"Skipping sensor {ds.get('name')} due to missing lat/lon")
            continue

        try:
            sensors.append(
                {
                    "dataset": ds.get("name"),
                    "station_id": extras.get("station_id"),
                    "lat": float(lat),
                    "lon": float(lon),
                }
            )
        except Exception:
            continue  # skip sensors with non-numeric coords

    if not sensors:
        raise RuntimeError(
            f"No valid sensor datasets with lat/lon found for region='{region}'"
        )

    # ------------------------------------------------------------------
    # 3) For each hotspot, find nearest sensor
    # ------------------------------------------------------------------
    has_ts = "timestamp" in hotspots.columns
    has_pm25 = "pm25_max" in hotspots.columns

    for _, row in hotspots.iterrows():

        nearest = None
        best_dist = float("inf")

        for s in sensors:
            d = haversine_km(row["lat"], row["lon"], s["lat"], s["lon"])
            if d < best_dist:
                best_dist = d
                nearest = s

        if nearest is None:
            continue

        ts_part = f"{row['timestamp']} | " if has_ts else ""
        pm_part = f"PM2.5 max={row['pm25_max']} → " if has_pm25 else "→ "

        print(
            f"[HOTSPOT] {ts_part}{pm_part}"
            f"{nearest['station_id']} ({best_dist:.1f} km)"
        )


if __name__ == "__main__":
    main()
