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
