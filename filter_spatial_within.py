import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import Generator

import fiona
import geopandas
from alive_progress import alive_bar
from shapely.geometry import shape


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
  
  # make the output folder if it does not exist
  output_folder_path = os.path.dirname(output_layer_path)
  if (not os.path.isdir(output_folder_path)): 
    os.makedirs(output_folder_path)
  
  with alive_bar(title='Parsing layers', total=2) as bar:
    filter_layer = fiona.open(filter_layer_path)
    filter_geom = [shape(feature['geometry']) for feature in filter_layer]
    bar()
    
    with fiona.open(input_layer_path) as layer:
      input_layer_crs = layer.crs
      bar() 
    
  def batched_records(batch_size: int | None = None) -> Generator[fiona.Feature, None, None]:
    with fiona.open(input_layer_path) as layer, alive_bar(len(layer), title='Filtering features') as bar, ProcessPoolExecutor() as executor:
      auto_batch_size = math.ceil(len(layer) / (cpu_count() - 1))
      
      futures = []
            
      # queue each batch of features to be filtered in a separate process
      for chunk in chunker(layer, batch_size if batch_size else auto_batch_size):
        future = executor.submit(__filter_features, chunk, filter_geom, invert)
        print(f'chunk size {len(chunk)}')
        future.add_done_callback(lambda future: bar(future.result()[1])) # increment the progress bar as each future completes
        futures.append(future)
      
      complete_batches = 0
      for future in as_completed(futures):
        complete_batches += 1
        print(f'completed {complete_batches} out of {len(chunk)} batches')
        None # wait for all futures to complete
      
      # yield the results of the futures in the order they were submitted
      # (we want to preserve order of features in the output layer)
      for future in futures:
        res = future.result()
        if res: yield res[0]

  def records():
    # flatten the batched records
    return [record for records in batched_records() for record in records]
  
  gdf = geopandas.GeoDataFrame.from_features(records(), crs=input_layer_crs)
  
  with alive_bar(title='Saving output layer') as bar:
    gdf.to_file(output_layer_path)
    bar()
  
def __filter_features(features, filter_geom, invert):
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