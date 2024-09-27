import os
import datetime
import logging
import hydra
from omegaconf import DictConfig
import numpy as np
import xarray as xr

from era5_mirror import ERA5Mirror

# @hydra.main(version_base="1.2", config_path="conf", config_name="config_uv")
def download_data() -> None:
    logging.getLogger().setLevel(logging.ERROR)  # Suppress logging from cdsapi
    zarr_store_path = "/datasets/zarr_data_uvsp_swh_mwp_2"
    mirror = ERA5Mirror(base_path=zarr_store_path)
    # split the years into train, validation, and test
    # all_years = list(range(cfg.start_train_year, cfg.end_train_year + 1)) + cfg.test_years + cfg.out_of_sample_years
    # Set the variables to download for 34 var dataset
    date_range = (
        datetime.date(2015, 9, 1),
        datetime.date(2015, 9, 1),
    )
    # area = '42/98/1/140'
    area = "90/-180/-90/180"
    dt = 6
    hours = [dt * i for i in range(0, 24 // dt)]
    # Start the mirror
    variables =  [
            '10m_u_component_of_wind', 
            # '10m_v_component_of_wind', 
            # 'mean_wave_period',
            # 'significant_height_of_combined_wind_waves_and_swell', 
            # 'surface_pressure',
    ]
    zarr_paths = mirror.download(variables, date_range, hours, area)
    print(f"Downloaded data saved to Zarr format at {zarr_store_path}")

if __name__ == "__main__":
    download_data()