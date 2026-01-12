# src/event_detection.py

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd
import yaml


@dataclass
class HotspotEvent:
    """
    A persistent hotspot detected at a single grid cell.
    """
    timestamp: pd.Timestamp   # time when persistence_k is reached
    lat: float
    lon: float
    pm25: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "lat": float(self.lat),
            "lon": float(self.lon),
            "pm25": float(self.pm25),
        }


def detect_hotspots_for_day(
    pred_csv: Path,
    pm25_col: str = "pm2p5_ugm3",
    threshold: float = 10.0,
    persistence_k: int = 3,
    timestep_hours: int = 1,
    round_decimals: int = 4,
) -> List[HotspotEvent]:
    """
    Detect persistent PM2.5 exceedance at the SAME grid cell (lat, lon).

    A hotspot is emitted only when:
      - pm25 >= threshold
      - for persistence_k consecutive timesteps
      - consecutive means timestamp difference == timestep_hours

    No bbox, no clustering.
    """

    if not pred_csv.exists():
        print(f"[event_detection] Missing predictions file: {pred_csv}")
        return []

    df = pd.read_csv(pred_csv)

    required = {"timestamp", "latitude", "longitude", pm25_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{pred_csv} missing required columns: {sorted(missing)}")

    # Normalize timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # ---- Create stable cell identifiers (avoid float noise) ----
    df["lat_key"] = df["latitude"].round(round_decimals)
    df["lon_key"] = df["longitude"].round(round_decimals)
    df["cell_id"] = df["lat_key"].astype(str) + "_" + df["lon_key"].astype(str)

    # Sort so streak logic works correctly
    df = df.sort_values(["cell_id", "timestamp"])

    expected_dt = pd.Timedelta(hours=timestep_hours)

    events: List[HotspotEvent] = []

    # ---- Process each grid cell independently ----
    for _, group in df.groupby("cell_id", sort=False):
        streak = 0
        prev_ts = None
        emitted = False

        for row in group.itertuples(index=False):
            ts = row.timestamp
            pm25 = float(getattr(row, pm25_col))
            above = pm25 >= threshold

            consecutive = (
                prev_ts is not None
                and (ts - prev_ts) == expected_dt
            )

            if above:
                if streak == 0 or not consecutive:
                    streak = 1
                    emitted = False
                else:
                    streak += 1
            else:
                streak = 0
                emitted = False

            # Emit event once when streak reaches persistence_k
            if (not emitted) and streak >= persistence_k:
                events.append(
                    HotspotEvent(
                        timestamp=ts,
                        lat=float(row.lat_key),
                        lon=float(row.lon_key),
                        pm25=pm25,
                    )
                )
                emitted = True

            prev_ts = ts

    return events


def events_to_dataframe(events: List[HotspotEvent]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["timestamp", "lat", "lon", "pm25"])
    return pd.DataFrame([e.to_dict() for e in events])


def main():
    """
    CLI: build hotspot CSVs for all dates in config.yaml.

    Writes:
        data/processed/hotspots/{date}_{region}_hotspots.csv
    """
    here = Path(__file__).resolve()
    project_root = here.parent.parent  # src/.. -> repo root
    config_path = project_root / "config.yaml"
    cfg = yaml.safe_load(config_path.read_text())

    region_name = cfg["region"]["name"]

    date_cfg = cfg["dates"]
    start = pd.to_datetime(date_cfg["start"])
    end = pd.to_datetime(date_cfg["end"])
    dates = pd.date_range(start, end, freq="D")

    times_tag = cfg["aurora"]["timestamp"]
    lead_hours = int(cfg["aurora"]["lead_hours"])

    hotspot_cfg = cfg.get("hotspots", {})
    threshold = float(hotspot_cfg.get("pm25_threshold", 10.0))
    persistence_k = int(hotspot_cfg.get("persistence_k", 3))
    timestep_hours = int(hotspot_cfg.get("timestep_hours", 1))
    round_decimals = int(hotspot_cfg.get("round_decimals", 4))

    out_base = project_root / "data/processed/hotspots"
    out_base.mkdir(parents=True, exist_ok=True)

    for day in dates:
        day_str = day.strftime("%Y-%m-%d")
        pred_csv = (
            project_root / "data/processed/predictions"
            / f"{day_str}_{times_tag}_{lead_hours}h_{region_name}.csv"
        )

        print(f"[event_detection] Processing {pred_csv}")

        events = detect_hotspots_for_day(
            pred_csv=pred_csv,
            threshold=threshold,
            persistence_k=persistence_k,
            timestep_hours=timestep_hours,
            round_decimals=round_decimals,
        )

        df_events = events_to_dataframe(events)

        df_events = (
            df_events
            .sort_values("timestamp")
            .drop_duplicates(subset=["lat", "lon"], keep="first")
        )
        
        out_path = out_base / f"{day_str}_{region_name}_hotspots.csv"
        df_events.to_csv(out_path, index=False)

        print(
            f"[event_detection] Found {len(df_events)} persistent hotspots → {out_path}"
        )


if __name__ == "__main__":
    main()
