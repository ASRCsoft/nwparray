---
icon: lucide/sun
---

# Overview

nwparray opens NWP weather forecast archives (such as those from NCEP or ECMWF)
as xarray datasets, so that the data can be organized for model training or
forecast evaluation. It's designed to be highly scalable via
[Dask](https://www.dask.org/), to quickly download and process terabytes of
archive data.

The package relies on templates from [Herbie](https://herbie.readthedocs.io/),
and uses many of the same function arguments.

!!! warning

    This package is in an early stage of development, so expect bugs and
	breaking changes.

## Example

```python
import pandas as pd
from nwparray import NwpCollection

# describe the HRRR data to get
searches = [
    ':TMP:2 m above ground:',
    ':DPT:2 m above ground:',
    ':PRES:surface:'
]
runs = pd.date_range(start='2021-04-01 12:00', periods=4, freq='D')
fxx = range(13) # forecast hours 0-12

hrrr = NwpCollection(runs, fxx, 'hrrr', searches=searches)
hrrr.collection_size() # estimate the complete download size

# create a list of xarray datasets
dataset_list = hrrr.open_datasets()
print(dataset_list[0])
```

```
<xarray.Dataset> Size: 793MB
Dimensions:  (time: 4, step: 13, y: 1059, x: 1799)
Coordinates:
  * time     (time) datetime64[us] 32B 2021-04-01T12:00:00 ... 2021-04-04T12:...
  * step     (step) int64 104B 0 1 2 3 4 5 6 7 8 9 10 11 12
  * y        (y) int64 8kB 0 1 2 3 4 5 6 ... 1052 1053 1054 1055 1056 1057 1058
  * x        (x) int64 14kB 0 1 2 3 4 5 6 ... 1792 1793 1794 1795 1796 1797 1798
Data variables:
    t2m      (time, step, y, x) float32 396MB dask.array<chunksize=(1, 13, 1059, 1799), meta=np.ndarray>
    d2m      (time, step, y, x) float32 396MB dask.array<chunksize=(1, 13, 1059, 1799), meta=np.ndarray>
```
