import os
import fiona
import geopandas
from alive_progress import alive_bar, alive_it
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
    
  def records():
    with fiona.open(input_layer_path) as layer:
      for feature in alive_it(layer, title='Filtering features'):
        feature_geom = shape(feature['geometry'])
        is_within_any_filter_layer_feature = feature_geom.within(filter_geom).any()
        
        if invert:
          is_within_any_filter_layer_feature = not is_within_any_filter_layer_feature
        
        if is_within_any_filter_layer_feature:
          yield feature
        else:
          continue
  
  gdf = geopandas.GeoDataFrame.from_features(records(), crs=input_layer_crs)
  
  with alive_bar(title='Saving output layer') as bar:
    gdf.to_file(output_layer_path)
    bar()
  
