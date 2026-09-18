---
icon: lucide/cloud-lightning
---

# Common tasks

## Getting parameter documentation

As of September 2026, for NCEP models such as the HRRR and GFS, parameter names
and descriptions are provided at <https://www.nco.ncep.noaa.gov/pmb/products/>.
(To get matching names in the xarray datasets, you must use the "grib2io"
engine.)

## Parallel processing

nwparray is set up to use [Dask](https://distributed.dask.org/en/stable/) for
parallel processing. The easiest way to set up Dask is to use the default
`LocalCluster` on a system with multiple CPU cores. Here's an example which will
run 4 threads in parallel:

```python
from dask.distributed import Client

# set up the dask workers for the LocalCluster
client = Client(processes=False, threads_per_worker=4,
                n_workers=1, memory_limit='2GB')
client.dashboard_link # view info about the tasks and workers
```

Many archives are stored on AWS, which encourages users to increase their
download speeds by downloading multiple files simultaneously. It can often
handle over 100 parallel downloads, with total download speeds on the order of
1GB/s. To reach these download speeds, you need a computer with high network
bandwidth and enough CPU cores to read the data files as they are downloaded.

!!! note

    Downloading files uses little CPU resources, so it often makes sense to run
	multiple threads per CPU core.

## Rechunking

Currently, nwparray's default chunking is not ideal for most uses. For example,
to get a time series of values at a single location, every chunk must be opened,
reading the entire data array.

The [rechunker](https://rechunker.readthedocs.io/) package can efficiently
convert the chunk sizes.

## Saving datasets

The datasets can easily be saved in zarr or netcdf formats. For example,

```python
nwpdataset.to_zarr("out.zarr", mode="w")
```

or 

```python
def get_nc_chunks(da):
    '''Get the netcdf chunk size by matching the dask chunk size, checking the
    array shape in case the array has been subsetted.
    '''
    return [ min(da.data.chunksize[i], da.shape[i]) for i in range(len(da.shape)) ]

encoding = { v: {"chunksizes": get_nc_chunks(nwpdataset[v]) } for v in nwpdataset.data_vars }
nwpdataset.to_netcdf("out.nc", encoding=encoding)
```

!!! warning

    By default, xarray's `to_netcdf` does not use the chunk sizes from Dask,
	which can lead to unexpected behavior. See [xarray issue
	8385](https://github.com/pydata/xarray/issues/8385).

When writing large datasets to netcdf, it often makes sense to use compression,
which can be set by adding it to the encoding argument:

```python
encoding = { v: {"zlib": True, "complevel": 5, "chunksizes": get_nc_chunks(nwpdataset[v])}
             for v in nwpdataset.data_vars }
```
