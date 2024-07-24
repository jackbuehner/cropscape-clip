import json
import os
import time
from typing import Any, Generator

import fiona
import geopandas
import pandas
import rich
from alive_progress import alive_bar, alive_it, config_handler

from calculate_pixel_trajectories import calculate_pixel_trajectories
from clip_cropscape_to_area_of_interest import \
    clip_cropscape_to_area_of_interest
from filter_spatial_within import filter_spatial_within
from reclassify_raster import PixelRemapSpecs, reclassify_rasters
from regrid_parcels_gdb_to_shp import regrid_parcels_gdb_to_shp
from summarize_raster import summarize_raster


def main(
  cropscape_input_folder: str,
  area_of_interest_shapefile: str,
  clipped_rasters_folder: str,
  consolidated_rasters_folder: str,
  reclass_spec: PixelRemapSpecs,
  parcels_shp_path: str,
  clipped_parcels_rasters_folder: str,
  parcels_summary_file: str,
  parcels_trajectories_file: str,
  parcels_gpkg_output_path: str
) -> None:
  start_time = time.time()

  console = rich.console.Console()
  status = console.status('[bold green]Working...[/bold green]')
  status.start()

  # limit to our area of interest by clipping first, which will also make subsequent steps faster
  status.update('Clipping cropscape data to area of interest...')
  clip_cropscape_to_area_of_interest(cropscape_input_folder, area_of_interest_shapefile, clipped_rasters_folder)
  console.log('Cropscape data clipped to area of interest')
  
  # Consolidate cropland data by reclassifying cropland data layer rasters
  # such that cropland is a single pixel value. Other pixel types are also
  # grouped together (e.g, all forest, all develolped land, all cropland, etc.).
  status.update('Consolidating cropland classes...')
  reclassify_rasters(clipped_rasters_folder[1:], consolidated_rasters_folder[1:], reclass_spec)
  console.log('Cropland classess consolidated')

  # create a list containing the paths to all consilidated rasters
  # so we can easily loop through them later
  consolidated_rasters_list = sorted([(f'{consolidated_rasters_folder}/{str}', int(str[0:4])) for str in os.listdir(consolidated_rasters_folder) if str.endswith("_30m_cdls.tif")], key=lambda x: x[1])

  # generate summary data for each cropland data year and parcel
  # and store it in the `summary_data` list
  status.update('Generating summary data for each cropland data year...')
  status.stop()
  summary_data = []
  summary_data += generate_summary_data(
                    consolidated_rasters_list[0:1],
                    parcels_shp_path,
                    clipped_parcels_rasters_folder,
                    status=status
                  )
  
  summary_data += generate_summary_data(
                    consolidated_rasters_list[-1:],
                    parcels_shp_path,
                    clipped_parcels_rasters_folder,
                    status=status
                  )
  
  summary_data += generate_summary_data(
                    consolidated_rasters_list[1:-1],
                    parcels_shp_path,
                    clipped_parcels_rasters_folder,
                    status=status
                  )
  
    
  console.log('Saving summary data for rasters within all input features...')  
  
  # save the `summary_data` list to JSON file
  summary_data_folder_path = os.path.dirname(parcels_summary_file)
  parcels_summary_file_root = os.path.splitext(parcels_summary_file)[0]
  if (not os.path.isdir(summary_data_folder_path)): 
    os.makedirs(summary_data_folder_path)
  with open(f'{parcels_summary_file_root}.json', "w") as file:
    with alive_bar(title='Saving summary data JSON', monitor=False):
      json.dump(summary_data, file, indent=2) 
    
  
  # create tidy data from the summary data
  tidy = []
  for entry in alive_it(summary_data, title="Creating tidy data"):
    year = entry['cropland_year']
    breakdowns = entry['data']['breakdown']
    breakdowns_with_year = [{'cropland_year': year, **breakdown} for breakdown in breakdowns]
    tidy += breakdowns_with_year
  with alive_bar(title='Creating data frame', monitor=False):
    tidy_df = pandas.json_normalize(tidy)
    # convert the id to a string with length 13
    tidy_df['id'] = tidy_df['id'].apply('{:0>13}'.format)
  
  # save the `summary_data` list to tidy CSV file
  with alive_bar(title='Saving tidy data CSV', monitor=False):
    tidy_df.to_csv(f'{parcels_summary_file_root}.csv', index=False)
      
  console.log(f'Summary data saved to {parcels_summary_file_root}.json')
  console.log(f'Tidy summary data saved to {parcels_summary_file_root}.csv')
  
  # generate trajectory data for each cropland data year and parcel
  trajectories = []
  with fiona.open(parcels_shp_path) as source:
    console.log(f'Generating trajectories for each feature within {parcels_shp_path}...')
    
    for feature in alive_it(source):
      trajectories.append({
        'parcelnumb': feature['properties']['parcelnumb'],
        'CDL_trajectories': calculate_pixel_trajectories(
          raster_folder_path=f'{clipped_parcels_rasters_folder}/{feature["properties"]["parcelnumb"]}',
          reclass_spec=reclass_spec,
          # output_trajectories_file=f'{clipped_parcels_rasters_folder}/{feature["properties"]["parcelnumb"]}/trajectories.json',
          temp_folder_path = f'./working/temp/trajectories/{feature["properties"]["parcelnumb"]}',
          # status=status
        )
      })
      
  console.log('Saving pixel trajectories data for features in {parcels_shp_path}...')  
  
  # save the `tidy_trajectories` list to JSON file
  trajectories_data_folder_path = os.path.dirname(parcels_trajectories_file)
  parcels_trajectories_file_root = os.path.splitext(parcels_trajectories_file)[0]
  if (not os.path.isdir(trajectories_data_folder_path)): 
    os.makedirs(trajectories_data_folder_path)
  with open(f'{parcels_trajectories_file_root}.json', "w") as file:
    with alive_bar(title='Saving trajectories data JSON', monitor=False):
      json.dump(trajectories, file, indent=2) 
  
  # save the `tidy_trajectories` list to tidy CSV file
  with alive_bar(title='Saving trajectories data CSV', monitor=False):
    trajectories_df = pandas.DataFrame(trajectories)
    # convert the id to a string with length 13
    trajectories_df['parcelnumb'] = trajectories_df['parcelnumb'].apply('{:0>13}'.format)
    trajectories_df.to_csv(f'{parcels_trajectories_file_root}.csv', index=False)
      
  console.log(f'Pixel trajectories saved to {parcels_summary_file_root}.json')
  console.log(f'Tidy pixel trajectories data saved to {parcels_summary_file_root}.csv')
  
  # join summary and trajectory data into parcels shapefile
  merged_gdf = join_pixel_counts_and_trajectories_to_features(
    parcels_shp_path=parcels_shp_path,
    tidy_df=tidy_df,
    trajectories_df=trajectories_df,
    reclass_spec=reclass_spec
  )
  
  # save the merged data to a geopackage
  merged_gdf.to_file(parcels_gpkg_output_path, layer='parcels', driver='GPKG')
  
  end_time = time.time()

  console.log(f'Elapsed time: {end_time - start_time} seconds ({(end_time - start_time) / 60} minutes)')

def generate_summary_data(
  consolidated_rasters_list: list[tuple[str, int]],
  parcels_shp_path: str,
  clipped_parcels_rasters_folder: str,
  *,
  status: rich.status.Status
) -> list[dict[str, object]]:
  """
  Summarizes the raster data within each parcel in the parcels shapefile and returns
  the results as a list of dictionaries with pixel counts and other metadata.
  """
  
  summary_data = []
  for (index, (file_path, year)) in enumerate(consolidated_rasters_list):
    file_root = os.path.splitext(file_path)[0]
      
    status.console.log(f'Summarizing raster within {parcels_shp_path} for {year}...')
          
    summary_data.append({
      'cropland_year': year,
      'data': summarize_raster(
        f'{file_root}.tif',
        None,
        parcels_shp_path,
        'parcelnumb',
        clipped_parcels_rasters_folder,
        # status=status,
        # status_prefix=f'[{year}] '
        show_progress_bar=True
      ) 
    })
    status.console.log(f'Summary data for {year} created') 
    
  return summary_data

def join_pixel_counts_and_trajectories_to_features(
  parcels_shp_path: geopandas.GeoDataFrame,
  tidy_df: pandas.DataFrame,
  trajectories_df: pandas.DataFrame,
  reclass_spec: PixelRemapSpecs,
) -> geopandas.GeoDataFrame:
  def records(layer: fiona.Collection) -> Generator[Any, Any, None]:
    """
    Processes each record in a layer and joins matching pixel counts to the feature properties.
    """
    for feature in alive_it(layer, title='Joining pixel counts to features'):
      # create a copy of the feature with only the id and geometry
      f = { k: feature[k] for k in ['id', 'geometry'] }
        
      pixel_summaries_tidy = tidy_df.loc[tidy_df['id'] == feature['properties']['parcelnumb']].copy()
      
      # calcualte a field that is the sum of all developed land classes
      pixel_summaries_tidy['developed_total'] = \
        pixel_summaries_tidy['pixel_counts.10'] if 'pixel_counts.10' in pixel_summaries_tidy.columns else 0 + \
        pixel_summaries_tidy['pixel_counts.11'] if 'pixel_counts.11' in pixel_summaries_tidy.columns else 0 + \
        pixel_summaries_tidy['pixel_counts.12'] if 'pixel_counts.12' in pixel_summaries_tidy.columns else 0 + \
        pixel_summaries_tidy['pixel_counts.13'] if 'pixel_counts.13' in pixel_summaries_tidy.columns else 0 + \
        pixel_summaries_tidy['pixel_counts.14'] if 'pixel_counts.14' in pixel_summaries_tidy.columns else 0
      
      # calcualte a field that is the sum of all cropland and grassland classes
      pixel_summaries_tidy['cropland_and_grassland_total'] = \
        pixel_summaries_tidy['pixel_counts.1'] if 'pixel_counts.1' in pixel_summaries_tidy.columns else 0 + \
        pixel_summaries_tidy['pixel_counts.3'] if 'pixel_counts.3' in pixel_summaries_tidy.columns else 0
      
      # pivot the data so that there is one row for each id
      # and there are two labels for each column
      # (cropland_year and one of the original columns)
      pixel_summaries_tidy = pixel_summaries_tidy.pivot(index='id', columns='cropland_year')
      
      # flatten the dual-label columns into a single-label column
      def rename(x):
        column, year = x
        
        if column.find('pixel_counts.') > -1:
          pixel_class = int(column.replace('pixel_counts.', ''))
          pixel_class_name = reclass_spec[254 if pixel_class == 0 else pixel_class]['name']
          return f'CDL{year}_{pixel_class_name}'
        
        return f'CDL{year}_{column}'
      pixel_summaries_tidy.columns = pixel_summaries_tidy.columns.map(rename)
      
      # merge the values of duplicate columns by transposing, grouping by
      # the first level, summing, and then transposing back
      # (class 254 and class 0 have the same column name)
      pixel_summaries_tidy = pixel_summaries_tidy.T.groupby(level=0).sum().T
      
      # join the pixel summaries to the feature properties
      pixel_summaries_dict = { key: value[feature['properties']['parcelnumb']] for key, value in pixel_summaries_tidy.to_dict().items()}
      f['properties'] = { **feature['properties'], **pixel_summaries_dict }
      
      yield f
  
  # open the parcels shapefile and join the pixel summaries to the features
  with fiona.open(parcels_shp_path) as layer:
    parcels_gdf = geopandas.GeoDataFrame.from_features(records(layer), crs=layer.crs)
  
  with alive_bar(title='Joining pixel trajectories to features', monitor=False):  
    # merge the trajectories data frame with the parcels features
    merged_gdf = geopandas.GeoDataFrame(
      parcels_gdf
        .merge(trajectories_df, on='parcelnumb')
    )
    
  return merged_gdf
  
if __name__ == '__main__':
  config_handler.set_global(title_length=32)

  # combine all parcel feature classes into a single shapefile
  # (this takes several hours to run, so comment out after running onces)
  print('Merging all parcel feature classes into a single shapefile...')
  regrid_parcels_gdb_to_shp(
    geodatabases_folder_path='./input/parcels',
    output_shp_folder_path='./working/parcels',
    columns_to_parse=['parcelnumb_no_formatting', 'lat', 'lon']
  )
  
  # create a new shapefile without urban area parcels
  # (this takes several hours to run, so comment out after running onces) 
  print('Filtering out urban area parcels...')
  filter_spatial_within(
    input_layer_path='./working/parcels.shp',
    filter_layer_path='./input/urban_areas_2020_corrected/urban_areas_2020_corrected.shp',
    output_layer_path='./working/parcels_rural.shp',
    invert=True
  )
  
  reclass_spec: PixelRemapSpecs = {
    254: { 'color': (0, 0, 0), 'name': 'background', 'original': [0] }, # we cannot have 0
    1: { 'color': (147, 105, 48), 'name': 'crops', 'original': list(range(1, 61)) + list(range(66, 81)) + list(range(195, 256) ) },
    2: { 'color': (100, 100, 100), 'name': 'idle', 'original': [61] },
    3: { 'color': (74, 59, 7), 'name': 'grassland', 'original': [62, 176] },
    4: { 'color': (53, 65, 22), 'name': 'forest', 'original': [63, 141, 142, 143] },
    5: { 'color': (78, 67, 27), 'name': 'shrubland', 'original': [64, 152] },
    6: { 'color': (50, 47, 36), 'name': 'barren', 'original': [65, 131] },
    10: { 'color': (195, 29, 20), 'name': 'developed', 'original': [82] },
    11: { 'color': (60, 32, 32), 'name': 'developed_open', 'original': [121] },
    12: { 'color': (106, 47, 31), 'name': 'developed_low', 'original': [122] },
    13: { 'color': (195, 29, 20), 'name': 'developed_med', 'original': [123] },
    14: { 'color': (139, 17, 11), 'name': 'developed_high', 'original': [124] },
    20: { 'color': (72, 93, 133), 'name': 'water', 'original': [83, 111, 112] },
    21: { 'color': (50, 103, 132), 'name': 'wetlands', 'original': [87, 190] },
    22: { 'color': (42, 45, 47), 'name': 'woody_wetlands', 'original': [190] },
    28: { 'color': (64, 76, 97), 'name': 'aquaculture', 'original': [92] },
    255: { 'color': (0, 0, 0), 'name': 'missing', 'original': [] }
  }
  
  # process cropland data with parcels
  main(
    cropscape_input_folder='./input', # folder containing cropland data layer rasters folders
    area_of_interest_shapefile='./input/area_of_interest.shp', # shapefile defining area of interest
    clipped_rasters_folder='./working/clipped', # folder for rasters clipped to area of interest
    consolidated_rasters_folder='./working/consolidated', # folder for consolidated cropland data layer rasters
    reclass_spec=reclass_spec,
    parcels_shp_path='./working/parcels_rural.shp',
    parcels_summary_file='./output/summary_data.json',
    clipped_parcels_rasters_folder='./working/clipped_parcels_sc_greenville_rural',
    parcels_trajectories_file='./output/trajectories.json',
    parcels_gpkg_output_path='./output/parcels.gpkg'
  )
