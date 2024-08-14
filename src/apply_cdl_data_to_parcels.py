from concurrent.futures import Future, ProcessPoolExecutor, as_completed
import json
import math
from multiprocessing import cpu_count
import os
import time
from typing import Any, Generator

import fiona
import geopandas
import pandas
import rich
from alive_progress import alive_bar, alive_it

from calculate_pixel_trajectories import calculate_pixel_trajectories
from clip_cropscape_to_area_of_interest import \
    clip_cropscape_to_area_of_interest
from multiprocess_counter import multiprocess_counter
from reclassify_raster import PixelRemapSpecs, reclassify_rasters
from summarize_raster import summarize_raster


def apply_cdl_data_to_parcels(
  cropscape_input_folder: str,
  area_of_interest_shapefile: str,
  clipped_rasters_folder: str,
  consolidated_rasters_folder: str,
  reclass_spec: PixelRemapSpecs,
  parcels_shp_path: str,
  id_key: str,
  clipped_parcels_rasters_folder: str,
  parcels_summary_file: str,
  parcels_trajectories_file: str,
  parcels_gpkg_output_path: str,
  *,
  skip_raster_clipping_and_reclassifying: bool = False,
  skip_summary_data: bool = False,
  skip_trajectories: bool = False,
) -> None:
  """
  Executes the main workflow for processing cropscape data and generating summary and trajectory data for parcels.
  
  Args:
    cropscape_input_folder (str): Path to the folder containing cropscape data.
    area_of_interest_shapefile (str): Path to the shapefile defining the area of interest.
    clipped_rasters_folder (str): Path to the folder where clipped rasters will be saved.
    consolidated_rasters_folder (str): Path to the folder where consolidated rasters will be saved.
    reclass_spec (PixelRemapSpecs): The pixel remap specifications for consolidating cropland classes.
    parcels_shp_path (str): Path to the shapefile containing parcel data.
    id_key (str): The column name with a unique identifier for each parcel.
    clipped_parcels_rasters_folder (str): Path to the folder where clipped parcel rasters will be saved.
    parcels_summary_file (str): Path to the file where the summary data will be saved.
    parcels_trajectories_file (str): Path to the file where the trajectory data will be saved.
    parcels_gpkg_output_path (str): Path to the output geopackage file.
    
  Returns:
    None
  """
  
  start_time = time.time()

  console = rich.console.Console()
  status = console.status('[bold green]Working...[/bold green]')
  status.start()

  if not skip_raster_clipping_and_reclassifying:
    # limit to our area of interest by clipping first, which will also make subsequent steps faster
    status.update('Clipping cropscape data to area of interest...')
    clip_cropscape_to_area_of_interest(cropscape_input_folder, area_of_interest_shapefile, clipped_rasters_folder)
    # console.log('Cropscape data clipped to area of interest')
  
    # Consolidate cropland data by reclassifying cropland data layer rasters
    # such that cropland is a single pixel value. Other pixel types are also
    # grouped together (e.g, all forest, all develolped land, all cropland, etc.).
    status.update('Consolidating cropland classes...')
    reclassify_rasters(clipped_rasters_folder, consolidated_rasters_folder, reclass_spec)
    # console.log('Cropland classess consolidated')

  if not skip_summary_data:
    # create a list containing the paths to all consilidated rasters
    # so we can easily loop through them later
    consolidated_rasters_list = sorted([(f'{consolidated_rasters_folder}/{str}', int(str[0:4])) for str in os.listdir(consolidated_rasters_folder) if str.endswith("_30m_cdls.tif")], key=lambda x: x[1])

    # generate summary data for each cropland data year and parcel
    # and store it in the `summary_data` list
    status.update('Generating summary data for each cropland data year...')
    status.stop()
    reordered_consolidated_rasters_list = consolidated_rasters_list[0:1] + consolidated_rasters_list[-1:] + consolidated_rasters_list[1:-1]
    summary_data =  list(
                      generate_summary_data(
                        reordered_consolidated_rasters_list,
                        parcels_shp_path,
                        clipped_parcels_rasters_folder,
                        id_key,
                        status=status,
                      )
                    )
    
      
    # console.log('Saving summary data for rasters within all input features...')
    
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
      # ensure id is a string
      tidy_df['id'] = tidy_df['id'].astype(str)
      # console.log(f'Summary data saved to {parcels_summary_file_root}.json')
    
    # save the `summary_data` list to tidy CSV file
    with alive_bar(title='Saving tidy data CSV', monitor=False):
      tidy_df.to_csv(f'{parcels_summary_file_root}.csv', index=False)
      # console.log(f'Tidy summary data saved to {parcels_summary_file_root}.csv')
      
    # join summary data to parcels shapefile
    merged_with_summaries_gdf = join_pixel_counts_to_featurs(
      parcels_shp_path=parcels_shp_path,
      tidy_df=tidy_df,
      reclass_spec=reclass_spec,
      id_key=id_key
    )
    
    # save the merged data to a geopackage
    with alive_bar(title=f'Saving parcels with CDL counts to geopackage {parcels_gpkg_output_path}', monitor=False):
      merged_with_summaries_gdf.to_file(parcels_gpkg_output_path, layer='Parcels with CDL counts', driver='GPKG', append=True)
    
    print(f'Elapsed time: {time.time() - start_time} seconds ({(time.time() - start_time) / 60} minutes)')

  # generate trajectory data for each cropland data year and parcel
  if not skip_trajectories:
    trajectories = []
    # console.log(f'Generating trajectories for each feature within {parcels_shp_path}...')
    parcels_gdf = geopandas.read_file(parcels_shp_path, engine='pyogrio', use_arrow=True)
    with alive_bar(len(parcels_gdf), title='Generating trajectories (slow)') as bar:
      
      with ProcessPoolExecutor(math.floor((cpu_count() - 1) / 2)) as executor:
        futures: list[tuple[Any, Future[dict[str, int]]]] = []
              
        for index, feature in parcels_gdf.iterrows():
          id_value = feature[id_key]
          parcelnumb = feature['parcelnumb']
          future = executor.submit(
                      calculate_pixel_trajectories,
                      f'{clipped_parcels_rasters_folder}/{parcelnumb}',
                      reclass_spec,
                      None, # f'{clipped_parcels_rasters_folder}/{feature["properties"]["parcelnumb"]}/trajectories.json',
                      f'./working/temp/trajectories/{parcelnumb}',
                      # status=status
                    )
          futures.append((id_value, future))
          
        for future in as_completed([future for (id_value, future) in futures]):
          bar()
          
        for (id_value, future) in futures:
          trajectories.append({
            id_key: id_value,
            'CDL_trajectories': future.result()
          })

    # console.log('Saving pixel trajectories data for features in {parcels_shp_path}...')  
    
    # save the `tidy_trajectories` list to JSON file
    trajectories_data_folder_path = os.path.dirname(parcels_trajectories_file)
    parcels_trajectories_file_root = os.path.splitext(parcels_trajectories_file)[0]
    if (not os.path.isdir(trajectories_data_folder_path)): 
      os.makedirs(trajectories_data_folder_path)
    with open(f'{parcels_trajectories_file_root}.json', "w") as file:
      with alive_bar(title='Saving trajectories data JSON', monitor=False):
        json.dump(trajectories, file, indent=2) 
        # console.log(f'Pixel trajectories saved to {parcels_summary_file_root}.json')
    
    # save the `tidy_trajectories` list to tidy CSV file
    with alive_bar(title='Saving trajectories data CSV', monitor=False):
      trajectories_df = pandas.DataFrame(trajectories)
      # convert the id to a string with length 13
      trajectories_df[id_key] = trajectories_df[id_key].apply('{:0>13}'.format)
      trajectories_df.to_csv(f'{parcels_trajectories_file_root}.csv', index=False)
      # console.log(f'Tidy pixel trajectories data saved to {parcels_summary_file_root}.csv')
        
    # join trajectory data to parcels shapefile
    merged_with_trajectories_gdf = join_pixel_trajectories_to_features(
      parcels_shp_path=parcels_shp_path,
      trajectories_df=trajectories_df,
      id_key=id_key
    )
    
    # save the merged data to a geopackage
    merged_with_trajectories_gdf.to_file(parcels_gpkg_output_path, layer='Parcels with CDL pixel trajectories', driver='GPKG', append=True)
    
    end_time = time.time()
    print(f'Elapsed time: {end_time - start_time} seconds ({(end_time - start_time) / 60} minutes)')

def generate_summary_data(
  consolidated_rasters_list: list[tuple[str, int]],
  parcels_shp_path: str,
  clipped_parcels_rasters_folder: str,
  id_key: str,
  *,
  status: rich.status.Status
) -> Generator[dict[str, object], None, None]:
  """
  Summarizes the raster data within each parcel in the parcels shapefile and returns
  the results as a list of dictionaries with pixel counts and other metadata.
  """
  
  # get the feature count for the shapefile
  with fiona.open(parcels_shp_path) as source:
    feature_count = len(list(source))
    
  # calculate the total features to be processed across all years
  total_features = feature_count * len(consolidated_rasters_list)
  
  with alive_bar(total_features, title='Generating summary data') as bar:
    with multiprocess_counter(lambda new_counter_value, old_counter_value: bar(new_counter_value - old_counter_value)) as (shared_counter, lock):
      with ProcessPoolExecutor() as executor:
        
        # queue each year as a separate process
        futures: list[tuple[int, Future[dict[str, Any]]]] = []
        for (file_path, year) in consolidated_rasters_list:
          file_root = os.path.splitext(file_path)[0]
          # print(f'Summarizing raster within {parcels_shp_path} for {year}...')
          future =  executor.submit(
                      summarize_raster,
                      f'{file_root}.tif',
                      None,
                      parcels_shp_path,
                      id_key,
                      clipped_parcels_rasters_folder,
                      # status=status,
                      # status_prefix=f'[{year}] ',
                      show_progress_bar=False,
                      shared_counter=shared_counter,
                      lock=lock
                    )
          # future.add_done_callback(lambda future: print(f'Finished raster within {parcels_shp_path} for {year}'))
          futures.append((year, future))

        # wait for all futures to complete
        while bar.current < total_features:
          time.sleep(0.1)
                          
        # yield the results of the futures in the order they were submitted
        # (we want to preserve order of features in the output layer)
        for (year, future) in futures:
          exception = future.exception()
          if exception:
            print(f'Error processing {year}: {exception}')
            raise exception
          
          data = future.result()
          if data: yield { 'cropland_year': year, 'data': data }

def join_pixel_counts_to_featurs(
  parcels_shp_path: geopandas.GeoDataFrame,
  tidy_df: pandas.DataFrame,
  reclass_spec: PixelRemapSpecs,
  id_key: str,
) -> geopandas.GeoDataFrame:
  
  def calculate_and_rename_columns() -> geopandas.GeoDataFrame:
    """
    Renames columns to use pixel class names and years.
    
    Also calculates a few extra columns for each feature.
    """
    pixel_summaries_tidy = tidy_df.copy()

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

    # show the index as a column
    # and use the id_key as the column name so that we can join the data to the parcels shapefile
    pixel_summaries_tidy[id_key] = pixel_summaries_tidy.index

    # make the id field the first field
    pixel_summaries_tidy = pixel_summaries_tidy[[id_key] + [col for col in pixel_summaries_tidy.columns if col != id_key]]

    return pixel_summaries_tidy

  with alive_bar(title='Opening parcels shapefile', monitor=False):
    parcels_gdf = geopandas.read_file(parcels_shp_path)
  
  with alive_bar(title='Processing summary columns', monitor=False):
    pixel_summaries_tidy = calculate_and_rename_columns()
  
  with alive_bar(title='Joining with summary data', monitor=False):  
    # merge the pixel summaries data frame with the parcels features
    merged_gdf = geopandas.GeoDataFrame(
      parcels_gdf
        .merge(pixel_summaries_tidy, on=id_key)
    )
    
  return merged_gdf
  
def join_pixel_trajectories_to_features(
  parcels_shp_path: geopandas.GeoDataFrame,
  trajectories_df: pandas.DataFrame,
  id_key: str,
) -> geopandas.GeoDataFrame:
  with alive_bar(title='Opening parcels shapefile', monitor=False):
    parcels_gdf = geopandas.read_file(parcels_shp_path)
  
  with alive_bar(title='Joining pixel trajectories to features', monitor=False):  
    # merge the trajectories data frame with the parcels features
    merged_gdf = geopandas.GeoDataFrame(
      parcels_gdf
        .merge(trajectories_df, on=id_key)
    )
    
  return merged_gdf
