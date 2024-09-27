import os
import xarray as xr
import dask
# import dask.distributed as Client #cdj
# from dask.diagnostics import ProgressBar
from progress import ProgressBar
import hydra
from omegaconf import DictConfig
import logging
import numpy as np

# from .era5_mirror import variable_to_zarr_name

@hydra.main(version_base="1.2", config_path="conf", config_name="config_uvsp_swh_mwp")
def generate_data(cfg: DictConfig) -> None:
	logging.getLogger().setLevel(logging.ERROR)  # Suppress logging from cdsapi


	# Open the zarr files and construct the xarray from them


	# Reformat the variables list so all elements are tuples
	reformated_variables = []
	for variable in cfg.variables:
		if isinstance(variable, str):
			reformated_variables.append(tuple([variable, None]))
		else:
			reformated_variables.append(variable)

	# Return the Zarr paths
	zarr_paths = []
	for variable, pressure_level in reformated_variables:
		zarr_path = f"{cfg.zarr_store_path}/{variable}.zarr"
		zarr_paths.append(zarr_path)
		
	# Check that Zarr arrays have correct dt for time dimension
	for zarr_path in zarr_paths:
		ds = xr.open_zarr(zarr_path)
		time_stamps = ds.time.values
		dt = time_stamps[1:] - time_stamps[:-1]
		assert np.all(
			dt == dt[0]
		), f"Zarr array {zarr_path} has incorrect dt for time dimension. An error may have occurred during download. Please delete the Zarr array and try again."


	zarr_arrays = [xr.open_zarr(path) for path in zarr_paths]
 
	#cdj interpolate the data to the same grid
	reference_lat = zarr_arrays[0].latitude
	reference_lon = zarr_arrays[0].longitude
 
	# zarr_arrays = [z.chunk('auto') for z in zarr_arrays]
	# client = Client(n_workers=64, threads_per_worker=2)

	# with dask.config.set(
	# 	scheduler="processes",
	# 	num_workers=64,
	# 	threads_per_worker=2,
	# 	**{"array.slicing.split_large_chunks": True},
	# ):
	# 	zarr_arrays[3] = zarr_arrays[3].interp(latitude=reference_lat, longitude=reference_lon).compute()
	# 	zarr_arrays[4] = zarr_arrays[4].interp(latitude=reference_lat, longitude=reference_lon).compute()
 
	zarr_arrays[3] = zarr_arrays[3].interp(latitude=reference_lat, longitude=reference_lon)
	zarr_arrays[4] = zarr_arrays[4].interp(latitude=reference_lat, longitude=reference_lon)

	era5_xarray = xr.concat(
		[z[list(z.data_vars.keys())[0]] for z in zarr_arrays], dim="channel"
	)
	era5_xarray = era5_xarray.transpose("time", "channel", "latitude", "longitude")
	era5_xarray.name = "fields"
	era5_xarray = era5_xarray.astype("float32")

	#cdj chunk the data
	# era5_xarray = era5_xarray.chunk({"time": 100, "channel": era5_xarray["channel"].shape[0], 
	#                               "latitude": era5_xarray["latitude"].shape[0], 
	#                               "longitude": era5_xarray["longitude"].shape[0]})
	era5_xarray = era5_xarray.chunk('auto')

	# if cfg.compute_mean_std:
	# 	era5_mean = np.array(
	# 		era5_xarray.mean(dim=("time", "latitude", "longitude")).values
	# 	)

	# 	era5_std = np.array(
	# 		era5_xarray.std(dim=("time", "latitude", "longitude")).values
	# 	)     
     	

	# split the years into train, validation, and test
	train_years = list(range(cfg.start_train_year, cfg.end_train_year + 1))
	test_years = cfg.test_years
	out_of_sample_years = cfg.out_of_sample_years
	all_years = train_years + test_years + out_of_sample_years

	means_all_years = []
	stds_all_years = []
	for year in all_years:
		# HDF5 filename
		split = (
			"train"
			if year in range(cfg.start_train_year, cfg.end_train_year + 1)
			else "test"
			if year in cfg.test_years
			else "out_of_sample"
		)
		#cdj
		if cfg.compute_mean_std:
			print(f"compute mean std {year}")
			with dask.config.set(
				scheduler="threads",
				num_workers=64,
				threads_per_worker=2,
				**{"array.slicing.split_large_chunks": True},
			):
				with ProgressBar():
					year_data = era5_xarray.sel(time=era5_xarray.time.dt.year == year)  
					era5_mean = np.array(
						year_data.mean(dim=("time", "latitude", "longitude")).values
					)
		
					era5_std = np.array(
						year_data.std(dim=("time", "latitude", "longitude")).values
					)
			means_all_years.append(era5_mean)
			stds_all_years.append(era5_std)
			print(f"Finished Computing {year}")


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
				year_data = era5_xarray.sel(time=era5_xarray.time.dt.year == year)  
             
				# Save data to a temporary local file
				year_data.to_netcdf(hdf5_path, engine="h5netcdf", compute=True)
		print(f"Finished Saving {year} at {hdf5_path}")
  
	# Save mean and std
	if cfg.compute_mean_std:
		stats_path = os.path.join(cfg.hdf5_store_path, "stats")
		print(f"Saving global mean and std at {stats_path}")
		if not os.path.exists(stats_path):
			os.makedirs(stats_path)

		# with dask.config.set(
		# 	scheduler="threads",
		# 	num_workers=64,
		# 	threads_per_worker=2,
		# 	**{"array.slicing.split_large_chunks": True},
		# ):
			# era5_mean = np.array(
			# 	era5_xarray.mean(dim=("time", "latitude", "longitude")).values
			# )
   
			# era5_std = np.array(
			# 	era5_xarray.std(dim=("time", "latitude", "longitude")).values
			# )
		overall_mean = np.mean(means_all_years, axis=0)
		overall_std = np.std(stds_all_years, axis=0)
  
		np.save(
			os.path.join(stats_path, "global_means.npy"), overall_mean.reshape(1, -1, 1, 1)
		)
    
		np.save(
			os.path.join(stats_path, "global_stds.npy"), overall_std.reshape(1, -1, 1, 1)
		)
		print(f"Finished saving global mean and std at {stats_path}")
  
if __name__ == "__main__":
	generate_data()