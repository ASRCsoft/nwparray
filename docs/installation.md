---
icon: lucide/cloud-sun
---

Installation requires git to be installed.

```sh
pip install git+https://github.com/ASRCsoft/nwparray
```

nwparray can use the NCEP tool
[grib2io](https://noaa-mdl.github.io/grib2io/grib2io.html) to read NCEP's custom
metadata codes, which are used for NCEP products like GFS, HRRR, etc. To use it,
install it from conda-forge with Conda or Pixi.

=== "Conda"

    ```sh
    conda install -c conda-forge grib2io
    ```

=== "Pixi"

    ```sh
    pixi add grib2io
    ```
