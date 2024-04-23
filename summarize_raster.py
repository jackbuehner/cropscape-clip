import functools
import json
import os
import shutil
from typing import Any

import geopandas
import numpy
import rasterio
import rich
from rasterio.io import DatasetReader
from rich.status import Status

from clip_raster import clip_raster

console = rich.console.Console()

def summarize_raster(input_raster_path: str, summary_output_path: str, feature_layer_path: str | None = None, breakdown_output_folder_path: str | None = None, *, status: Status | None = None, status_prefix: str = '') -> dict[str, Any]:
  '''
  Generate summary metadata for an input raster.
  - pixel counts for each class
  - area for each class (if area unit is provided)
  - breakdown by feature (if feature class layer and breakdown output folder path is provided)
  
  The breakdown by feature class will generate metadata for each feature in
  the feature class.
  
  *Only singleband rasters are supported*
  
  Args:
    input_raster_path (str): The path to the input raster.
    summary_output_path (str): The path to the output json file.
    feature_layer_path (str | None): The path to the feature layer to use for breakdown. If None, no breakdown will be generated.
    breakdown_output_folder_path (str | None): The path to the folder where breakdown tiff and json files will be saved. If None, no breakdown will be generated.
    
  Returns:
    dict[str, Any]: The summary metadata for the input raster.
  '''
  
  if status: status.start()
  
  # open the raster and lock it in the filesystem while working on it
  if status: status.update(f'{status_prefix}Opening input raster...')
  raster: DatasetReader = rasterio.open(input_raster_path)
  if status: status.console.log(f'{status_prefix}Raster opened')
      
  # we only look at band 1 -- multiband rasters are not supported
  band1: numpy.ndarray[Any, Any] = raster.read(1)
      
  # count the number of pixels for each class and put them into a dictionary
  if status: status.update(f'{status_prefix}Parsing raster pixels...')
  clipped_pixel_classes, clipped_pixel_counts = numpy.unique(band1, return_counts=True)
  clipped_pixel_class_counts = dict(zip(clipped_pixel_classes.tolist(), clipped_pixel_counts.tolist()))
  if status: status.console.log(f'{status_prefix}Raster pixels parsed')
  
  if feature_layer_path:
    breakdown_metadata = process_feature_layer(raster, feature_layer_path, 'ZCTA5CE10', breakdown_output_folder_path, status=status, status_prefix=status_prefix)
    
   # metadata for the feature
  feature_metadata = {
    # 'ID': row['ID'],
    # 'Area': row['Area'],
    'total_pixels': int(numpy.sum(clipped_pixel_counts)),
    'pixel_counts': clipped_pixel_class_counts,
    'breakdown': breakdown_metadata
  }
    
  # save the feature metadata to json
  if status: status.update(f'{status_prefix}Saving metadata...')
  with open(summary_output_path, "w") as file:
    json.dump(feature_metadata, file, indent=2) 
    if status: status.console.log(f'{status_prefix}Metadata saved to {summary_output_path}')
  
  # remove the lock on the raster
  raster.close()
  
  return feature_metadata

@functools.cache
def read_feature_layer(feature_layer_path: str) -> geopandas.GeoDataFrame:
  '''
  Open a feature layer from file path and return it as a GeoDataFrame.
  This function's result is cached to prevent multiple reads of the same file.
  '''
  return geopandas.read_file(feature_layer_path)
  
def process_feature_layer(raster: DatasetReader, feature_layer_path: str, id_key: str, output_folder_path: str | None = None, *, status: Status | None = None, status_prefix: str = '') -> list[dict[str, Any]]:
  raster_root, raster_ext = os.path.splitext(raster.name)
  raster_name = os.path.basename(raster_root)
  feature_layer_root, feature_layer_ext = os.path.splitext(feature_layer_path)
  feature_layer_name = os.path.basename(feature_layer_root)
      
  # open the vector feature layer
  if status: status.update(f'{status_prefix}Opening feature layer...')
  feature_layer = read_feature_layer(feature_layer_path)
  if status: status.console.log(f'{status_prefix}feature layer loaded')
  
  # the zip codes that belong to south carolina are those that start with '29'
  if status: status.update(f'{status_prefix}Filtering feature layer to South Carolina...')
  feature_layer = feature_layer[feature_layer['ZCTA5CE10'].str.startswith('29')]
  feature_layer = feature_layer.reset_index(drop=True)
  if status: status.console.log(f'{status_prefix}Filtered feature layer to South Carolina')
  
  # loop through each feature in the feature layer
  breakdowns: list[dict[str, Any]] = []
  for index, row in feature_layer.iterrows():
    id = row[id_key]
    loop_status_prefix = f'{status_prefix}[{id}] '
    
    # create output folder
    _output_folder_path = output_folder_path or './temp' # if the output folder path is not provided, use a temporary folder
    output_folder = f'{_output_folder_path}/{id}'
    output_raster_file = f'{output_folder}/{raster_name}__{feature_layer_name}.tiff'
    output_json_file = f'{output_folder}/{raster_name}__{feature_layer_name}.json'
    if (not os.path.isdir(output_folder)):
      if status: status.update(f'{loop_status_prefix}Creating folder {output_folder}...')
      os.makedirs(output_folder)
      if status: status.console.log(f'{loop_status_prefix}Folder {output_folder} created')
    
    # clip, process, and save
    out_image, out_transform, out_meta, out_colormap = clip_raster(raster, feature_layer, index, status=status, status_prefix=loop_status_prefix)
    with rasterio.open(output_raster_file, "w", **out_meta) as dest:
      # get the clipped band 1
      clipped_band1 = out_image[0]

      # count the number of pixels for each class in the clipped band
      if status: status.update(f'{loop_status_prefix}Parsing raster pixels...')
      clipped_pixel_classes, clipped_pixel_counts = numpy.unique(clipped_band1, return_counts=True)
      clipped_pixel_class_counts = dict(zip(clipped_pixel_classes.tolist(), clipped_pixel_counts.tolist()))
      if status: status.console.log(f'{loop_status_prefix}Raster pixels parsed')

      # generate metadata for the feature
      feature_metadata = {
        # 'ID': row['ID'],
        # 'Area': row['Area'],
        'id': id,
        'total_pixels': int(numpy.sum(clipped_pixel_counts)),
        'pixel_counts': clipped_pixel_class_counts
      }
      
      # save the feature metadata to json
      if status: status.update(f'{loop_status_prefix}Saving metadata...')
      breakdowns.append(feature_metadata)
      if output_folder_path is not None:
        with open(output_json_file, "w") as file:
          json.dump(feature_metadata, file, indent=2) 
          if status: status.console.log(f'{loop_status_prefix}Metadata saved to {output_json_file}')
      
      # write the image
      if output_folder_path is not None:
        if status: status.update(f'{loop_status_prefix}Writing clipped raster...')
        dest.write(out_image[0], 1)
        dest.write_colormap(1, out_colormap)
        if status: status.console.log(f'{loop_status_prefix}Clipped raster saved to {output_raster_file}')
    
    # clean up temp feature folder
    if output_folder_path is None:
      if status: status.update(f'{loop_status_prefix}Deleting temp folder {output_folder}...')
      shutil.rmtree(output_folder)
      if status: status.console.log(f'{loop_status_prefix}Temp folder {output_folder} deleted')
  
  # clean up main temp folder
  if output_folder_path is None:
    if status: status.update(f'{status_prefix}Deleting temp folder {_output_folder_path}...')
    shutil.rmtree(_output_folder_path)
    if status: status.console.log(f'{status_prefix}Temp folder {_output_folder_path} deleted')
    
  return breakdowns
