# src/event_detection.py

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import yaml


@dataclass
class HotspotEvent:
    event_id: str
    timestamp: pd.Timestamp
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    pm25_max: float
    pm25_mean: float
    num_cells: int

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "lat_min": self.lat_min,
            "lat_max": self.lat_max,
            "lon_min": self.lon_min,
            "lon_max": self.lon_max,
            "pm25_max": float(self.pm25_max),
            "pm25_mean": float(self.pm25_mean),
            "num_cells": int(self.num_cells),
        }


def detect_hotspots_for_day(
    pred_csv: Path,
    pm25_col: str = "pm2p5_ugm3",
    threshold: float = 5.0,
    min_cells: int = 5,
) -> List[HotspotEvent]:
    """
    - Load Aurora daily predictions CSV
    - For each timestamp:
        * select grid cells with pm2p5_ugm3 >= threshold
        * if at least min_cells cells exceed threshold:
            create ONE hotspot whose bbox covers those cells
    """
    if not pred_csv.exists():
        print(f"[event_detection] Missing predictions file: {pred_csv}")
        return []

    df = pd.read_csv(pred_csv)
    if "timestamp" not in df.columns or "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError(f"{pred_csv} does not have required columns")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    if pm25_col not in df.columns:
        raise ValueError(f"{pred_csv} missing PM2.5 column '{pm25_col}'")

    events: List[HotspotEvent] = []

    # group by hour (assuming Aurora already hourly; you can change to df['timestamp'].dt.floor('H'))
    for ts, group in df.groupby("timestamp"):
        high = group[group[pm25_col] >= threshold]
        if len(high) < min_cells:
            continue

        lat_min = high["latitude"].min()
        lat_max = high["latitude"].max()
        lon_min = high["longitude"].min()
        lon_max = high["longitude"].max()

        pm25_max = high[pm25_col].max()
        pm25_mean = high[pm25_col].mean()

        event_id = f"{ts.strftime('%Y%m%dT%H%M')}_{int(lat_min*100):+05d}_{int(lon_min*100):+06d}"

        events.append(
            HotspotEvent(
                event_id=event_id,
                timestamp=ts,
                lat_min=lat_min,
                lat_max=lat_max,
                lon_min=lon_min,
                lon_max=lon_max,
                pm25_max=pm25_max,
                pm25_mean=pm25_mean,
                num_cells=len(high),
            )
        )

    return events


def events_to_dataframe(events: List[HotspotEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(
            columns=[
                "event_id",
                "timestamp",
                "lat_min",
                "lat_max",
                "lon_min",
                "lon_max",
                "pm25_max",
                "pm25_mean",
                "num_cells",
            ]
        )
    return pd.DataFrame([e.to_dict() for e in events])


def main():
    """
    CLI: build hotspot CSVs for all dates in config.yaml.

    Writes:
        data/processed/hotspots/{region}/{date}_hotspots.csv
    """
    config_path = Path("config.yaml")
    cfg = yaml.safe_load(config_path.read_text())

    region_name = cfg["region"]["name"]
    date_cfg = cfg["dates"]
    start = pd.to_datetime(date_cfg["start"])
    end = pd.to_datetime(date_cfg["end"])
    dates = pd.date_range(start, end, freq="D")

    times_tag = cfg["aurora"]["timestamp"]        # e.g. "0000-1200"
    lead_hours = int(cfg["aurora"]["lead_hours"]) # e.g. 12

    threshold = float(cfg.get("hotspots", {}).get("pm25_threshold", 35.0))
    min_cells = int(cfg.get("hotspots", {}).get("min_cells", 5))

    out_base = Path("data/processed/hotspots") / region_name
    out_base.mkdir(parents=True, exist_ok=True)

    for day in dates:
        day_str = day.strftime("%Y-%m-%d")
        pred_csv = (
            Path("data/processed/predictions")
            / region_name
            / f"{day_str}_{times_tag}_{lead_hours}h.csv"
        )

        print(f"[event_detection] Processing {pred_csv}")
        events = detect_hotspots_for_day(pred_csv, threshold=threshold, min_cells=min_cells)
        df_events = events_to_dataframe(events)

        out_path = out_base / f"{day_str}_hotspots.csv"
        df_events.to_csv(out_path, index=False)
        print(f"[event_detection] Found {len(df_events)} hotspots → {out_path}")


if __name__ == "__main__":
    main()
