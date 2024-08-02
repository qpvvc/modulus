import os
import xarray as xr
import dask
from dask.diagnostics import ProgressBar
import hydra
from omegaconf import DictConfig
import logging
import numpy as np

# from .era5_mirror import variable_to_zarr_name

@hydra.main(version_base="1.2", config_path="conf", config_name="config_uv_1")
def generate_data(cfg: DictConfig) -> None:
	logging.getLogger().setLevel(logging.ERROR)  # Suppress logging from cdsapi


	# Open the zarr files and construct the xarray from them


	# Reformat the variables list so all elements are tuples
	# reformated_variables = []
	# for variable in cfg.variables:
	# 	if isinstance(variable, str):
	# 		reformated_variables.append(tuple([variable, None]))
	# 	else:
	# 		reformated_variables.append(variable)
 
	# split the years into train, validation, and test
	train_years = list(range(cfg.start_train_year, cfg.end_train_year + 1))
	test_years = cfg.test_years
	out_of_sample_years = cfg.out_of_sample_years
	all_years = train_years + test_years + out_of_sample_years
    
	all_years = [2015]
	# Return the Zarr paths
	grib_paths = []
	for year in all_years:
		for month in range(8,10):
			grib_path = f"{cfg.grib_store_path}/{year}/wnd10m.cdas1.{year}{month:02d}.grb2"
			grib_paths.append(grib_path)
  
		
	# Check that Zarr arrays have correct dt for time dimension
	# for zarr_path in zarr_paths:
	# 	ds = xr.open_zarr(zarr_path)
	# 	time_stamps = ds.time.values
	# 	dt = time_stamps[1:] - time_stamps[:-1]
	# 	assert np.all(
	# 		dt == dt[0]
	# 	), f"Zarr array {zarr_path} has incorrect dt for time dimension. An error may have occurred during download. Please delete the Zarr array and try again."


	grib_arrays = [xr.open_dataset(path) for path in grib_paths]
 
	cfsr_xarray_concat = xr.concat(
		grib_arrays, dim="time"
	)
	cfsr_xarray_channel = xr.concat(
		[cfsr_xarray_concat[z] for z in list(cfsr_xarray_concat.data_vars.keys())], dim="channel"
	)
	cfsr_xarray_stacked =  cfsr_xarray_channel.stack(newtime=['time', 'step'])
	cfsr_xarray_stacked = cfsr_xarray_stacked.swap_dims({'newtime':'valid_time'})
	cfsr_xarray_drop = cfsr_xarray_stacked.drop_vars('newtime')
	cfsr_xarray = cfsr_xarray_drop.transpose("valid_time", "channel", "latitude", "longitude")
	cfsr_xarray.name = "fields"
	cfsr_xarray = cfsr_xarray.astype("float32")

	# Save mean and std
	if cfg.compute_mean_std:
		stats_path = os.path.join(cfg.hdf5_store_path, "stats")
		print(f"Saving global mean and std at {stats_path}")
		if not os.path.exists(stats_path):
			os.makedirs(stats_path)

		if os.path.exists(os.path.join(stats_path, "global_means.npy")):
			print("Skipping mean and std calculation as global_means.npy and global_stds.npy already exist")
		else:
			era5_mean = np.array(
				cfsr_xarray.mean(dim=("valid_time", "latitude", "longitude")).values
			)
			np.save(
				os.path.join(stats_path, "global_means.npy"), era5_mean.reshape(1, -1, 1, 1)
			)

		if os.path.exists(os.path.join(stats_path, "global_stds.npy")):
			print("Skipping mean and std calculation as global_means.npy and global_stds.npy already exist")
		else:			
			era5_std = np.array(
				cfsr_xarray.std(dim=("valid_time", "latitude", "longitude")).values
			)
			np.save(
				os.path.join(stats_path, "global_stds.npy"), era5_std.reshape(1, -1, 1, 1)
			)
		print(f"Finished saving global mean and std at {stats_path}")


	# Make hdf5 files
	for year in all_years:
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
		# Check if the file already exists
		if os.path.exists(hdf5_path):
			print(f"Skipping {year} as {hdf5_path} already exists")
			continue
		# Save year using dask
		print(f"Saving {year} at {hdf5_path}")
		with dask.config.set(
			scheduler="threads",
			num_workers=64,
			threads_per_worker=2,
			**{"array.slicing.split_large_chunks": False},
		):
			with ProgressBar():
				# Get data for the current year
				year_data = cfsr_xarray.sel(valid_time=cfsr_xarray.valid_time.dt.year == year)
				# Save data to a temporary local file
				year_data.to_netcdf(hdf5_path, engine="h5netcdf", compute=True)
		print(f"Finished Saving {year} at {hdf5_path}")

if __name__ == "__main__":
	generate_data()