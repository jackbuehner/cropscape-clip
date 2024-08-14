Read a single parcel feature layer from an ESRI geodatabase, split it into chunks, calculate cropland data layer pixel coverage for each parcel, and save it to a GeoPackage.

Usage:

```bash
python main.py --gdb_path <path_to_geodatabase> --layer_name <feature_layer_name> --id_key <unique_identifier_column> --output_gpkg <output_geopackage_path> [--chunk_size <chunk_size>] [--filter_layer_path <filter_layer_path>] [--cdls_folder_path <cdls_folder_path>] [--cdls_aoi_shp_path <cdls_aoi_shp_path>] [--invert-filter <invert_filter>] [--summary_output_folder_path <summary_output_folder_path>]
```

Arguments:

- `gdb_path` (required): Path to the ESRI geodatabase.
- `layer_name` (required): Name of the feature layer.
- `id_key` (required): Column name of the unique identifier for the parcels. Will be truncated to 10 characters.
- `output_gpkg` (required): Path to the output GeoPackage.
- `chunk_size` (optional): Number of features per chunk (default is 50000).
- `filter_layer_path` (optional): The file path to a shapefile to filter out features. The filter is a spatial within. Can be inverted with --invert-filter.
- `cdls_folder_path` (optional): Path to folder containing the folders for each year of the Cropland Data Layer named with the format 'YYYY_30m_cdls'.
- `cdls_aoi_shp_path` (optional): Path to a shapefile specifying the area of interest for the Cropland Data Layers. They will be cropped to the extent of this shapefile.
- `invert-filter (optional)`: Invert the filter condition.
- `summary_output_folder_path` (optional): Folder to save the summary data.
- `skip_remove_to` (optional): Skip removing the input/output folders.
- `skip_processing` (optional): Skip processing the feature layer.
- `skip_merge` (optional): Skip merging the feature layers.

## Building the image

```bash
docker build . -t cropscape-clip
```

```bash
docker save -o ./cropscape-clip.tar cropscape-clip
```

## Using the image

```bash
docker load -i ./cropscape-clip.tar
```

```bash
docker run -it --rm\
  --name cropscape-clip\
  --mount type=bind,source="$(pwd)"/logging,target=/home/app/logging\
  --mount type=bind,source="$(pwd)"/input,target=/home/app/input\
  --mount type=bind,source="$(pwd)"/output,target=/home/app/output\
  cropscape-clip
```

Then, run `python main.py` with the arguments you need. To see valid arguments, run `python main.py --help`.

Example:

```bash
python main.py\
  --gdb_path=input/parcels/sc_greenville.gdb\
  --layer_name=sc_greenville \
  --id_key=parcelnumb_no_formatting\
  --output_gpkg=output/sc_greenville.gpkg\
  --filter_layer_path=input/urban_areas_2020_corrected/urban_areas_2020_corrected.shp\
  --invert-filter=true\
  --cdls_folder_path=input/cdls\
  --cdls_aoi_shp_path=input/south_carolina/area_of_interest.shp\
  --summary_output_folder_path=output
```
