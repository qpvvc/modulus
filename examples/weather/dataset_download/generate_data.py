import os
import xarray as xr
import dask
from dask.diagnostics import ProgressBar
import hydra
from omegaconf import DictConfig
import logging
import numpy as np

@hydra.main(version_base="1.2", config_path="conf", config_name="config_tas")
def generate_data(cfg: DictConfig) -> None:
	logging.getLogger().setLevel(logging.ERROR)  # Suppress logging from cdsapi
	# Open the zarr files and construct the xarray from them
	zarr_paths = [os.path.join(cfg.zarr_store_path, f"{year}.zarr") for year in range(cfg.start_train_year, cfg.end_train_year + 1) + cfg.test_years + cfg.out_of_sample_years]
	zarr_arrays = [xr.open_zarr(path) for path in zarr_paths]
	era5_xarray = xr.concat(
		[z[list(z.data_vars.keys())[0]] for z in zarr_arrays], dim="channel"
	)
	era5_xarray = era5_xarray.transpose("time", "channel", "latitude", "longitude")
	era5_xarray.name = "fields"
	era5_xarray = era5_xarray.astype("float32")

	# Save mean and std
	if cfg.compute_mean_std:
		stats_path = os.path.join(cfg.hdf5_store_path, "stats")
		print(f"Saving global mean and std at {stats_path}")
		if not os.path.exists(stats_path):
			os.makedirs(stats_path)
		era5_mean = np.array(
			era5_xarray.mean(dim=("time", "latitude", "longitude")).values
		)
		np.save(
			os.path.join(stats_path, "global_means.npy"), era5_mean.reshape(1, -1, 1, 1)
		)
		era5_std = np.array(
			era5_xarray.std(dim=("time", "latitude", "longitude")).values
		)
		np.save(
			os.path.join(stats_path, "global_stds.npy"), era5_std.reshape(1, -1, 1, 1)
		)
		print(f"Finished saving global mean and std at {stats_path}")

	# Make hdf5 files
	for year in range(cfg.start_train_year, cfg.end_train_year + 1) + cfg.test_years + cfg.out_of_sample_years:
		# HDF5 filename
		split = (
			"train"
			if year in range(cfg.start_train_year, cfg.end_train_year + 1)
			else "test"
			if year in cfg.test_years
			else "out_of_sample"
		)
		hdf5_path = os.path.join(cfg.hdf5_store_path, split)
		os.makedirs(hdf5_path, exist_ok=True)
		hdf5_path = os.path.join(hdf5_path, f"{year}.h5")
		# Save year using dask
		print(f"Saving {year} at {hdf5_path}")
		with dask.config.set(
			scheduler="threads",
			num_workers=8,
			threads_per_worker=2,
			**{"array.slicing.split_large_chunks": False},
		):
			with ProgressBar():
				# Get data for the current year
				year_data = era5_xarray.sel(time=era5_xarray.time.dt.year == year)
				# Save data to a temporary local file
				year_data.to_netcdf(hdf5_path, engine="h5netcdf", compute=True)
		print(f"Finished Saving {year} at {hdf5_path}")

if __name__ == "__main__":
	generate_data()