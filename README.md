nwparray opens NWP weather forecast archives (such as those from NCEP or ECMWF)
as xarray datasets, so that the data can be organized for model training or
forecast evaluation. It's designed to be highly scalable via Dask, to quickly
download and process terabytes of archive data.

The package relies on templates from [Herbie](https://herbie.readthedocs.io/),
and uses many of the same function arguments.

This package is in an early stage of development, so expect bugs and breaking
changes.

# Example

```python
import pandas as pd
from nwparray import NwpCollection
from dask.distributed import Client

# set up the dask workers
client = Client(processes=False, threads_per_worker=4,
                n_workers=1, memory_limit='2GB')
client.dashboard_link # view info about the tasks and workers

# describe the GEFS data to get
searches = [
    ':TMP:2 m above ground:',
    ':DPT:2 m above ground:',
    ':PRES:surface:'
]
runs = pd.date_range(start='2021-04-01 12:00', periods=4, freq='D')
fxx = range(3, 24 * 8, 3) # 63 forecast hours going out 8 days

gefs_0p25 = NwpCollection(runs, fxx, 'gefs', 'atmos.25', searches,
                          members=['avg'])
gefs_0p25.collection_size() # estimate the complete download size

# The data can be opened in xarray, much like with cfgrib
dataset_list = gefs_0p25.open_datasets()
```

# Installation

Installation requires git to be installed.

```sh
pip install git+https://github.com/ASRCsoft/nwparray
```

# Benchmarks

On a kubernetes cluster with a high-speed internet connection, running 600
threads, I was able to download GEFS data from AWS at an average of 700MB/s
(5.6Gb/s).
