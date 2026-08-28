---
icon: lucide/cloud-rain
---

# Common tasks

## Parallel processing

nwparray is set up to use Dask for parallel processing.

```python
from dask.distributed import Client

# set up the dask workers
client = Client(processes=False, threads_per_worker=4,
                n_workers=1, memory_limit='2GB')
client.dashboard_link # view info about the tasks and workers
```

## Rechunking

[rechunker](https://rechunker.readthedocs.io/) package
