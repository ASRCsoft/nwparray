'''Organize collections of NWP files.
'''

import warnings, tempfile
from humanize import naturalsize
import numpy as np
import xarray as xr
import dask.array as da
from herbie import Herbie
from .nwppath import NwpPath
from .nwpdownloader import NwpDownloader

class NwpCollection:
    '''A (potentially large) collection of NWP data files.

    Arguments are generally the same as `Herbie` and `FastHerbie` from Herbie.

    Args:
        DATES : pandas-parsable datetime string or list of datetimes
        fxx : int or pandas-parsable timedelta (e.g. "6h")
            Forecast lead time *in hours*. Available lead times depend on
            the model type and model version.
        model : {'hrrr', 'hrrrak', 'rap', 'gfs', 'ecmwf', etc.}
            Model name as defined in the models template folder.
            CASE INSENSITIVE; e.g., "HRRR" is the same as "hrrr".
        product : {'sfc', 'prs', 'nat', 'subh', etc.}
            Output variable product file type. If not specified, will
            use first product in model template file. CASE SENSITIVE.
            For example, the HRRR model has these products:
            - ``'sfc'`` surface fields
            - ``'prs'`` pressure fields
            - ``'nat'`` native fields
            - ``'subh'`` subhourly fields
        searches : list of str
            List of regular expressions to subset the file by specific variables
            and levels. Each string must match a single variable. Read more
            in the Herbie user guide:
            https://herbie.readthedocs.io/en/latest/user_guide/tutorial/search.html
        member : None or int
            Some ensemble models (e.g. the future RRFS) will need to
            specify an ensemble member.
        engine : {'cfgrib', 'grib2io'}
            xarray engine to use for reading the files. Defaults to 'cfgrib'
            (the ECMWF package). 'grib2io', an NCEP package, uses NCEP's custom
            grib2 metadata codes.

    '''
    
    def __init__(self, DATES, fxx, model, searches, product=None, members=None,
                 engine='cfgrib'):
        '''Create an `NwpCollection`.
        '''
        self.DATES = DATES
        self.model = model
        self.product = product
        self.searches = searches
        self.fxx = fxx
        self.members = members
        self.engine = engine

    def _download(self, tmp_dir, search, *args, **kwargs):
        '''Download an NWP grib2 file using Herbie.
        '''
        tmp_archive = NwpDownloader(*args, **kwargs, save_dir=tmp_dir,
                                    model=self.model, product=self.product,
                                    verbose=False)
        # herbie will drive me insane with messages if I don't create this
        # directory
        tmp_path = tmp_archive.get_localFilePath()
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        full_file = tmp_archive.download(search)
        return full_file

    @property
    def file_size(self):
        '''Use the inventory to calculate the file size.
        '''
        # this temp directory prevents Herbie from reading from local index
        # files, which may not match the source file
        with tempfile.TemporaryDirectory() as tmp_dir:
            if self.members is None:
                h = Herbie(self.DATES[0], model=self.model,
                           product=self.product, fxx=self.fxx[0],
                           save_dir=tmp_dir, verbose=False)
            else:
                h = Herbie(self.DATES[0], model=self.model,
                           product=self.product, fxx=self.fxx[0],
                           member=self.members[0], save_dir=tmp_dir,
                           verbose=False)
        search_string = '|'.join(self.searches)
        inv = h.inventory(search=search_string)
        return (inv['end_byte'] - inv['start_byte'] + 1).sum()

    def collection_size(self, humanize=True):
        '''Calculate the size of the collection.
        '''
        if self.members is None:
            n_files = len(self.DATES) * len(self.fxx)
        else:
            n_files = len(self.DATES) * len(self.fxx) * len(self.members)
        full_download_size = self.file_size * n_files
        if humanize:
            return naturalsize(full_download_size)
        else:
            return full_download_size

    def _download_and_extract_single(self, coords, search, out_dir):
        '''Download a single variable from a single file using the coordinates
        from the download status matrix.
        '''
        date = self.DATES[coords[0]]
        fxx = self.fxx[coords[1]]
        if self.members is None:
            member = None
        else:
            member = self.members[coords[2]]
        out_path = self._download(out_dir, search, date, fxx=fxx, member=member)
        return out_path

    def _read_single_variable(self, f, engine):
        if engine == 'cfgrib':
            # backend_kwargs = {'filter_by_keys': var_conf['filter_by_keys'],
            #                   'indexpath': ''}
            ds = xr.open_dataset(f, engine='cfgrib',
                                 decode_timedelta=True)
                                 #backend_kwargs=backend_kwargs)
        elif engine == 'grib2io':
            # backend_kwargs = {'save_index': False, 'filters': {'shortName': 'HGT'}}
            ds = xr.open_dataset(f, engine='grib2io')
                                 #backend_kwargs=backend_kwargs)
            ds.load()
        return ds

    def _array_from_coords_remote(self, coords, var_conf, search):
        '''Get grib array from run/fxx/member coordinates, downloading the file
        rather than reading it from a local path.
        '''
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                grib_path = self._download_and_extract_single(coords, search, out_dir=tmp_dir)
                ds = self._read_single_variable(grib_path, engine=self.engine)
                out = ds[var_conf['name']].values
            except Exception as e:
                # convert the error to a warning, and return an empty array
                warnings.warn(str(e))
                out = np.full(var_conf['shape'], np.nan)
        # check that the array has the correct shape
        if out.shape != var_conf['shape']:
            warnings.warn('Array has incorrect shape')
            out = np.full(var_conf['shape'], np.nan)
        return out

    def _members_arr(self, coords, var_conf, search):
        return np.stack([ self._array_from_coords_remote(coords + (i, ), var_conf, search)
                          for i in range(len(self.members)) ])

    def _fxx_arr_map(self, var_conf, search, block_id=None, block_info=None):
        if self.members is None:
            out = np.stack([ self._array_from_coords_remote((block_id[0], i), var_conf, search)
                             for i in range(len(self.fxx)) ])
        else:
            out = np.stack([ self._members_arr((block_id[0], i), var_conf, search)
                             for i in range(len(self.fxx)) ])
        return np.expand_dims(out, 0)

    # Rather than create a dask array for each file, create one for each
    # forecast run. This is much more manageable for the dask scheduler.
    def _delayed_collection_arr(self, var_conf, search):
        if self.members is None:
            coords = {'time': self.DATES, 'step': self.fxx}
        else:
            coords = {'time': self.DATES, 'step': self.fxx, 'number': self.members}
        if self.engine == 'cfgrib':
            ignored_dims = set(["time", "step", "number", "valid_time"])
        elif self.engine == 'grib2io':
            ignored_dims = set(["refDate", "leadTime", "perturbationNumber", "validDate"])
        extra_dims = { v: var_conf['dims'][v] for v in var_conf['dims'].keys()
                       if not v in ignored_dims }
        coords.update(extra_dims)
        if self.members is None:
            fxx_shape = (len(self.fxx), ) + var_conf['shape']
        else:
            fxx_shape = (len(self.fxx), len(self.members)) + var_conf['shape']
        # use `map_blocks` instead of `stack`
        n_runs = len(self.DATES)
        arr = da.map_blocks(self._fxx_arr_map, var_conf, search,
                            dtype=var_conf['dtype'],
                            chunks=((1, ) * n_runs, *fxx_shape),
                            meta=np.array((), dtype=var_conf['dtype']))
        out_da = xr.DataArray(arr, coords=coords, name=var_conf['name'])
        # Add back the other original coordinates, but not if they depend on
        # ["time", "step", "number"] because these all changed relative to the
        # reference data file. Also have to drop ("reset") coordinates on the
        # coordinates themselves, because otherwise they will be propogated up
        # to the array coordinates.
        extra_coords = { v: var_conf['coords'][v].reset_coords(drop=True) for v in var_conf['coords'].keys()
                         if (not v in ignored_dims) and ignored_dims.isdisjoint(var_conf['coords'][v].dims) }
        return out_da.assign_coords(extra_coords)

    def open_array(self, search):
        '''Returns an xarray DataArray with data from the entire file
        collection, where the array is a dask array.

        search : str
            Use regex to subset the file by specific variables and levels. Must
            match a single variable. Read more in the Herbie user guide:
            https://herbie.readthedocs.io/en/latest/user_guide/tutorial/search.html
        '''
        # read a single grib2 file to get coordinates and attributes
        # Getting info about the dataset--
        # - array shape (x/y coordinates)
        # - variable info for filter_by_keys
        # - attributes
        # this is similar to `_array_from_coords_remote`
        coords = (0, 0, 0)
        with tempfile.TemporaryDirectory() as tmp_dir:
            # get the file
            grib_path = self._download_and_extract_single(coords, search, out_dir=tmp_dir)
            ds = self._read_single_variable(grib_path, engine=self.engine)
        attr_dict = {}
        conf_list = []
        for v in ds.data_vars:
            conf = {'name': v}
            arr = ds[v]
            conf['shape'] = arr.shape
            conf['dtype'] = arr.dtype
            dim_names = list(arr.dims)
            conf['dims'] = { dim: arr[dim] for dim in dim_names }
            conf['coords'] = { coord: arr[coord] for coord in arr.coords }
            conf_list.append(conf)
            attr_dict[v] = arr.attrs
        # as long as we get data separately for each vertical level, we can
        # easily merge the arrays. But I haven't guaranteed that yet!
        arrs = []
        conf = conf_list[0]
        arr = self._delayed_collection_arr(conf, search)
        arr.attrs = attr_dict[arr.name]
        return arr

    # inspired by `cfgrib.open_datasets`
    def open_datasets(self, searches=None):
        '''Analogous to `cfgrib.open_datasets`, but for a collection of remote
        files. Open the file collection as a list of xarray datasets, one for
        each incompatible set of data dimensions. Each list item is an xarray
        dataset with data from the entire file collection, where the data arrays
        are dask arrays.

        searches : list of str
            List of regular expressions to subset the file by specific variables
            and levels. Each string must match a single variable. Read more
            in the Herbie user guide:
            https://herbie.readthedocs.io/en/latest/user_guide/tutorial/search.html
        '''
        if searches is None:
            searches = self.searches
        arrs = [ self.open_array(s) for s in searches ]
        type_of_level_arrays = {}
        for arr in arrs:
            type_of_level = arr.attrs.get("GRIB_typeOfLevel", "undef")
            type_of_level_arrays.setdefault(type_of_level, []).append(arr)
        merged = []
        for type_of_level in sorted(type_of_level_arrays):
            ds_list = [ arr.to_dataset() for arr in type_of_level_arrays[type_of_level] ]
            merged.append(xr.merge(ds_list, join="exact",
                                   combine_attrs="identical"))
        return merged
