# nwparray

[![Tests](https://github.com/ASRCsoft/nwparray/actions/workflows/test.yml/badge.svg)](https://github.com/ASRCsoft/nwparray/actions/workflows/test.yml)

nwparray opens NWP weather forecast archives (such as those from NOAA or ECMWF)
as xarray datasets, so that the data can be organized for model training or
forecast evaluation. It's designed to be highly scalable via
[Dask](https://www.dask.org/), to quickly download and process terabytes of
archive data.

See the full documentation at <https://asrcsoft.github.io/nwparray>.


# Example

```python
import pandas as pd
from nwparray import NwpCollection

# describe the HRRR data to get
searches = [
    ':TMP:2 m above ground:',
    ':DPT:2 m above ground:'
]
runs = pd.date_range(start='2021-04-01 12:00', periods=4, freq='D')
fxx = range(13) # forecast hours 0-12

hrrr = NwpCollection(runs, fxx, 'hrrr', searches=searches)
hrrr.collection_size() # estimate the complete download size

# create a list of xarray datasets
dataset_list = hrrr.open_datasets()
print(dataset_list[0])
```

# Installation

Installation requires git to be installed.

```sh
pip install git+https://github.com/ASRCsoft/nwparray
```
