import os

import fiona
import gdaltools
from alive_progress import alive_bar, alive_it


def geodatabases_to_geopackage(geodatabases_folder_path: str, output_gpkg_path: str, *, columns_to_parse: list[str] = ['parcelnumb_no_formatting', 'lat', 'lon']) -> str: 
  """
  Converts regrid parcel data from multiple single-layer geodatabases to one multi-layer geopackage. This may take a while to run.
  
  Args:
    geodatabases_folder_path (str): The path to the folder containing the geodatabases. Nothing else should exist in this folder.
    output_shp_folder_path (str): The path to the folder where the output shapefiles will be saved.
    columns_to_parse (list[str], optional): The list of column names to parse from the geodatabases. Defaults to ['parcelnumb_no_formatting', 'lat', 'lon'].
  
  Returns:
    str: The path to the combined shapefile containing all the parcels.
  """
  
  # error if the output file does not end with .gpkg
  if not output_gpkg_path.endswith('.gpkg'):
    raise ValueError('The output file path must end with .gpkg')
    
  # make the output folder if it does not exist
  output_folder_path = os.path.dirname(output_gpkg_path)
  if (not os.path.isdir(output_folder_path)): 
    os.makedirs(output_folder_path)
  
  geodatabase_paths = [f'{geodatabases_folder_path}/{name}' for name in os.listdir(geodatabases_folder_path) if name.endswith('.gdb')]
  geodatabase_paths_length = len(geodatabase_paths)
  print(f'Found {geodatabase_paths_length} geodatabase files in {geodatabases_folder_path}')
  
  # determine the target crs
  layer_info = str(gdaltools.ogrinfo(geodatabase_paths[0], fiona.listlayers(geodatabase_paths[0])[0], summary=True, fields=False))
  epsg_index = layer_info.index('ID["EPSG"')
  srs = layer_info[epsg_index+4:epsg_index+14].replace('",', ':')
    
  # create the combined geopackage
  for index, geodatabase_path in enumerate(alive_it(geodatabase_paths, title='Merging geodatabases')):
    layer_name = fiona.listlayers(geodatabase_path)[0]
    
    ogr = gdaltools.ogr2ogr()
    ogr.set_encoding("UTF-8")
    if index > 0: ogr.set_output_mode('AP', 'UP') # append mode for all but the first geodatabase
    
    ogr.set_input(geodatabase_path, layer_name)
    ogr.set_output(output_gpkg_path, 'GPKG', layer_name, srs)
    
    # limit the attribute table columns to only the ones we want
    # to increase processing speeds
    ogr.set_sql(f'SELECT {", ".join(columns_to_parse)} FROM {layer_name}') 
  
    ogr.execute()
      
  return output_gpkg_path
