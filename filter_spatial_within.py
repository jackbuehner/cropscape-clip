import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import Any, Generator

import fiona
import geopandas
from alive_progress import alive_bar
from shapely.geometry import shape

from multiprocess_counter import multiprocess_counter


def filter_spatial_within(input_layer_path: str, filter_layer_path: str, output_layer_path: str, *, invert: bool = False) -> None:
  """
  Filters features from an input layer that are spatially within any feature of a filter layer.
  
  The input layer and filter layer must have the same CRS.
  
  Args:
    input_layer_path (str): The file path to the input layer.
    filter_layer_path (str): The file path to the filter layer.
    output_layer_path (str): The file path to save the output layer.
    invert (bool, optional): If True, invert the filter condition. Defaults to False.
  
  Returns:
    None
  """
  
  start_time = time.time()
  
  # error if not shp or gpkg file
  if not (input_layer_path.endswith('.shp') or input_layer_path.endswith('.gpkg')):
    raise ValueError('The input layer must be a shapefile or geopackage')
  
  # error if the input and output layers are the same
  if input_layer_path == output_layer_path:
    raise ValueError('The input and output layers cannot be the same')
  
  # error if the input and output layer file types are different
  if input_layer_path[-4:] != output_layer_path[-4:]:
    raise ValueError('The input and output layer file types must be the same')
  
  # error if the filter layer is not a shapefile
  if not filter_layer_path.endswith('.shp'):
    raise ValueError('The filter layer must be a shapefile')
    
  # make the output folder if it does not exist
  output_folder_path = os.path.dirname(output_layer_path)
  if (not os.path.isdir(output_folder_path)): 
    os.makedirs(output_folder_path)
    
  # if the input is a geopackage, process each layer individually
  if input_layer_path.endswith('.gpkg'):
    layer_names = sorted(fiona.listlayers(input_layer_path))
    for index, layer_name in enumerate(layer_names):
       process_layer(input_layer_path, filter_layer_path, output_layer_path, invert=invert, layer_name=layer_name, current=index + 1, total=len(layer_names))
  
  # otherwise, directly process the input shapefile layer
  else:
    process_layer(input_layer_path, filter_layer_path, output_layer_path, invert=invert)
  
  end_time = time.time()
  print(f'Finished in {end_time - start_time:.2f} seconds ({(end_time - start_time) / 60:.2f} minutes)')

def process_layer(input_layer_path: str, filter_layer_path: str, output_layer_path: str, *, invert: bool = False, layer_name: str | None = None, current: int = 1, total: int = 1) -> None:
  monitor=('{count}/{total} [{percent:.0%}]' + f' ⟨{layer_name} – {current}/{total}⟩') if layer_name else '{count}/{total} [{percent:.0%}]'
  
  # parse the input and filter layers
  with alive_bar(title='Parsing layers', total=2, monitor=monitor) as bar:
    filter_layer = fiona.open(filter_layer_path)
    filter_geom = [shape(feature['geometry']) for feature in filter_layer]
    bar()
    
    with fiona.open(input_layer_path, layer=layer_name) as layer:
      input_layer_crs = layer.crs
      bar() 
    
  def batched_records(batch_size: int | None = None) -> Generator[fiona.Feature, None, None]:
    
    with fiona.open(input_layer_path, layer=layer_name) as layer:
      with alive_bar(len(layer), title='Filtering features', monitor=monitor) as bar:
        with multiprocess_counter(lambda new_counter_value, old_counter_value: bar(new_counter_value - old_counter_value)) as (shared_counter, lock), ProcessPoolExecutor() as executor:
          auto_batch_size = math.ceil(len(layer) / (cpu_count() - 1))
          
          futures = []
        
          # queue each batch of features to be filtered in a separate process
          for chunk in chunker(layer, batch_size if batch_size else auto_batch_size):
            future = executor.submit(__filter_features, chunk, filter_geom, invert, shared_counter, lock)
            
            # print(f'chunk size {len(chunk)}')
            futures.append(future)
          
          complete_batches = 0
          for future in as_completed(futures):
            complete_batches += 1
            # print(f'completed {complete_batches} out of {len(chunk)} batches')
            None # wait for all futures to complete
          
      with alive_bar(len(futures), title='Compiling chunks', monitor=monitor) as bar:          
        # yield the results of the futures in the order they were submitted
        # (we want to preserve order of features in the output layer)
        for future in futures:
          res = future.result()
          if res: 
            bar()
            yield res[0]

  records = [record for records in batched_records() for record in records]
  
  with alive_bar(title='Creating GeoDataFrame', monitor=False) as bar:
    gdf = geopandas.GeoDataFrame.from_features(records, crs=input_layer_crs)
  
  with alive_bar(title='Saving to layer to package' if layer_name else 'Saving output layer', monitor=False) as bar:
    gdf.to_file(output_layer_path, layer=layer_name, driver='GPKG' if output_layer_path.endswith('.gpkg') else 'ESRI Shapefile')
    bar()

def __filter_features(features, filter_geom, invert, shared_counter, lock):
  """
  Filters a list of features based on whether they are within a given filter geometry.

  Args:
    features (list): A list of features to filter.
    filter_geom (geometry): The filter geometry to check against.
    invert (bool): If True, invert the filter result.

  Returns:
    tuple: A tuple containing the filtered features and the total number of features that were input for filtering.
  """
  features_to_return = []
  
  for feature in features:
    feature_geom = shape(feature['geometry'])
    is_within_any_filter_layer_feature = feature_geom.within(filter_geom).any()
    
    if invert:
      is_within_any_filter_layer_feature = not is_within_any_filter_layer_feature
    
    if is_within_any_filter_layer_feature:
      features_to_return.append(feature)
      
    with lock: shared_counter.value += 1

  return (features_to_return, len(features))

def chunker(seq, size):
  """
  Splits a sequence into chunks of a specified size.

  Args:
    seq (sequence): The sequence to be chunked.
    size (int): The size of each chunk.

  Returns:
    generator: A generator that yields chunks of the specified size from the sequence.

  Example:
    >>> list(chunker([1, 2, 3, 4, 5, 6, 7, 8, 9], 3))
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
  """
  return (seq[pos:pos + size] for pos in range(0, len(seq), size))