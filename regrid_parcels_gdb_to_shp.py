import os
import time

import fiona
import geopandas
import pandas
from alive_progress import alive_it


def regrid_parcels_gdb_to_shp(geodatabases_folder_path: str, output_shp_folder_path: str, *, columns_to_parse: list[str] = ['parcelnumb_no_formatting', 'lat', 'lon']) -> str: 
  """
  Converts regrid parcel data from geodatabases to shapefiles. This may take a while to run.
  
  Args:
    geodatabases_folder_path (str): The path to the folder containing the geodatabases. Nothing else should exist in this folder.
    output_shp_folder_path (str): The path to the folder where the output shapefiles will be saved.
    columns_to_parse (list[str], optional): The list of column names to parse from the geodatabases. Defaults to ['parcelnumb_no_formatting', 'lat', 'lon'].
  
  Returns:
    str: The path to the combined shapefile containing all the parcels.
  """
  
  start_time = time.time()
  
  # make the output folder if it does not exist
  if (not os.path.isdir(output_shp_folder_path)): 
    os.makedirs(output_shp_folder_path)
  
  geodatabase_paths = [f'{geodatabases_folder_path}/{name}' for name in os.listdir(geodatabases_folder_path) if name.endswith('.gdb')]
  geodatabase_paths_length = len(geodatabase_paths)
  print(f'Found {geodatabase_paths_length} geodatabase files in {geodatabases_folder_path}')
  
  geodataframes = []
  for index, geodatabase_path in enumerate(geodatabase_paths):
    layer_name = fiona.listlayers(geodatabase_path)[0]
    gdf = geodataframe_from_geodatabase(geodatabase_path, layer_name, columns_to_parse, status_suffix=f' ({index + 1}/{len(geodatabase_paths)})')
    geodataframes.append(gdf)
    gdf.to_file(f'{output_shp_folder_path}/{layer_name}.shp')
    
  output_combined_shapefile_path = f'{output_shp_folder_path}/parcels.shp'
  geopandas.GeoDataFrame(pandas.concat(geodataframes)).to_file(output_combined_shapefile_path)
  
  end_time = time.time()
  print(f'total time: {end_time - start_time} seconds ({(end_time - start_time) / 60} minutes)')
  
  return output_combined_shapefile_path


def geodataframe_from_geodatabase(geodatabase_path: str, layer_name: str, columns: list[str], *, status_prefix: str = '', status_suffix: str = '') -> geopandas.GeoDataFrame:
  """
  This function reads a layer from a geodatabase and returns a GeoDataFrame.

  Args:
    geodatabase_path (str): The path to the geodatabase file.
    layer_name (str): The name of the layer to read from the geodatabase.
    columns (list): A list of column names to read from the layer.

  Returns:
    geopandas.GeoDataFrame: A GeoDataFrame containing the data from the layer.
  """
  print(f'{status_prefix}Processing {geodatabase_path.split("/")[-1]}/{layer_name}{status_suffix}...')
  
  crs = ''
  with fiona.open(geodatabase_path, layer=layer_name) as source:
    crs = source.crs
    
  def records():
    with fiona.open(geodatabase_path, layer=layer_name) as source:      
      for feature in alive_it(source, title=f'Reading features'):
        
        # create a copy of the feature with only the id and geometry
        f = { k: feature[k] for k in ['id', 'geometry'] }
        
        # if columns is provided, only added those columns to the properties
        # otherwise, add all columns
        if columns:
          f['properties'] = { k: feature['properties'][k] for k in columns }
        else:
          f['properties'] = feature['properties']
        
        # yield instead of return since we are in a generator
        # and we want the result to continue to be a generator
        yield f
      
  return geopandas.GeoDataFrame.from_features(records(), crs=crs)