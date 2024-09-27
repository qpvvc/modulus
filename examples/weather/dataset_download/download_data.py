import os
import datetime
import logging
import hydra
from omegaconf import DictConfig
import numpy as np
import xarray as xr

from era5_mirror import ERA5Mirror

@hydra.main(version_base="1.2", config_path="conf", config_name="config_uv")
def download_data(cfg: DictConfig) -> None:
	logging.getLogger().setLevel(logging.ERROR)  # Suppress logging from cdsapi
	mirror = ERA5Mirror(base_path=cfg.zarr_store_path)
	# split the years into train, validation, and test
	all_years = list(range(cfg.start_train_year, cfg.end_train_year + 1)) + cfg.test_years + cfg.out_of_sample_years
	# Set the variables to download for 34 var dataset
	date_range = (
		datetime.date(min(all_years), 1, 1),
		datetime.date(max(all_years), 12, 31),
	)
	hours = [cfg.dt * i for i in range(0, 24 // cfg.dt)]
	# Start the mirror
	zarr_paths = mirror.download(cfg.variables, date_range, hours)
	print(f"Downloaded data saved to Zarr format at {cfg.zarr_store_path}")

if __name__ == "__main__":
	download_data()