from pathlib import Path
import os
import requests
import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()
SYNOPTIC_BASE_URL = "https://api.synopticdata.com/v2/stations/timeseries"


def build_time_strings(start: pd.Timestamp, end: pd.Timestamp) -> tuple[str, str]:
    """
    Synoptic time format: YYYYMMDDHHMM (UTC).
    We include the full end-date by going to end + 1 day at 00:00.
    """
    start_utc = start.tz_convert("UTC") if start.tzinfo else start.tz_localize("UTC")
    end_plus = end + pd.Timedelta(days=1)
    end_utc = end_plus.tz_convert("UTC") if end_plus.tzinfo else end_plus.tz_localize("UTC")

    start_str = start_utc.strftime("%Y%m%d%H%M")
    end_str = end_utc.strftime("%Y%m%d%H%M")
    return start_str, end_str


def download_synoptic_station_timeseries(
    station_id: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    out_path: Path,
):
    """
    Download Synoptic timeseries for one station and date range, save as CSV.

    The request asks for:
      - PM2.5, Ozone, CO, NO2
    """
    token = os.environ.get("SYNOPTIC_TOKEN")
    if not token:
        raise RuntimeError("SYNOPTIC_TOKEN environment variable not set")

    start_str, end_str = build_time_strings(start, end)

    params = {
        "token": token,
        "stid": station_id,
        "start": start_str,
        "end": end_str,
        "obtimezone": "UTC",
        "units": "metric",
        "output": "csv",
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        print(f"[Synoptic] File already exists for {station_id}: {out_path}")
        return

    print(f"[Synoptic] Downloading {station_id} from {start_str} to {end_str} → {out_path}")
    resp = requests.get(SYNOPTIC_BASE_URL, params=params, timeout=60)
    resp.raise_for_status()

    # Save raw CSV response
    out_path.write_text(resp.text)
    print(f"[Synoptic] Saved: {out_path}")


def main():
    # config.yaml is at project root (one level up from src/)
    CONFIG_PATH = "config.yaml"
    cfg = yaml.safe_load(open(CONFIG_PATH))

    date_cfg = cfg["dates"]
    start = pd.to_datetime(date_cfg["start"])
    end = pd.to_datetime(date_cfg["end"])

    stations = cfg["stations"]

    out_dir = Path("data/raw/stations_raw")

    start_tag = start.strftime("%Y%m%d")
    end_tag = end.strftime("%Y%m%d")

    for st in stations:
        stid = st["station_id"]
        out_path = out_dir / f"{stid}_{start_tag}_{end_tag}.csv"
        download_synoptic_station_timeseries(stid, start, end, out_path)


if __name__ == "__main__":
    main()
