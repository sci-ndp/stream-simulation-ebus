import yaml
import pandas as pd
from pathlib import Path

from aurora_runner import run_aurora_forecast


def main():
    # This file lives in src/, config.yaml is one level up
    here = Path(__file__).resolve()
    project_root = here.parent.parent
    config_path = project_root / "config.yaml"

    config = yaml.safe_load(open(config_path))

    region_cfg = config["region"]
    date_cfg = config["dates"]
    aurora_cfg = config["aurora"]
    paths_cfg = config["paths"]

    # Build date list from start/end
    start = pd.to_datetime(date_cfg["start"])
    end = pd.to_datetime(date_cfg["end"])
    date_list = [d.strftime("%Y-%m-%d") for d in pd.date_range(start, end, freq="D")]

    # Aurora / CAMS settings
    time_tag = aurora_cfg["timestamp"]       # e.g. "0000-1200"
    lead_hours = int(aurora_cfg["lead_hours"])

    # Bounding box
    bbox = dict(
        lat_min=region_cfg["lat_min"],
        lat_max=region_cfg["lat_max"],
        lon_min=region_cfg["lon_min"],
        lon_max=region_cfg["lon_max"],
    )

    # Paths
    cams_dir = (project_root / Path(paths_cfg["cams_download"])).expanduser().resolve()
    static_path = cams_dir / paths_cfg["static_pickle"]

    out_dir = project_root / "data/processed/predictions"
    out_dir.mkdir(parents=True, exist_ok=True)

    for date in date_list:
        surf_path = cams_dir / f"{date}_{time_tag}_{lead_hours}h_surface-level.nc"
        atmo_path = cams_dir / f"{date}_{time_tag}_{lead_hours}h_atmospheric.nc"

        out_csv = out_dir / f"{date}_{time_tag}_{lead_hours}h_{region_cfg['name']}.csv"

        print(f"Running Aurora for {date} → {out_csv}")
        run_aurora_forecast(
            surf_path=surf_path,
            atmo_path=atmo_path,
            static_path=static_path,
            out_csv=out_csv,
            bbox=bbox,
        )


if __name__ == "__main__":
    main()
