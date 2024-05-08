import json
import os
import time

import rich

from clip_cropscape_to_area_of_interest import \
    clip_cropscape_to_area_of_interest
from compute_raster_class_difference import PixelDiffSpecs, compute_raster_class_difference
from reclassify_raster import PixelRemapSpecs, reclassify_rasters
from summarize_raster import summarize_raster

CROPSCAPE_INPUT_FOLDER = './input' # folder containing cropland data layer rasters folders
AREA_OF_INTEREST_SHAPEFILE = './input/area_of_interest.shp' # shapefile defining area of interest
CLIPPED_RASTERS_FOLDER = './working/clipped' # folder for rasters clipped to area of interest

CONSOLIDATED_RASTERS_FOLDER = './output/consolidated' # folder for consolidated cropland data layer rasters

ZCTA_SHAPES_FOLDER = './input/zcta' # folder containing ZCTA shapefiles
ZCTA_CLIPPED_AND_SUMMARY_STATS_FOLDER = './working/zcta' # folder for consolidated cropland data layer rasters clipped to each zcta shape and their summary data
ZCTA_SUMMARY_FILE = './output/summary_data.json' # file for summary data for each cropland data year (consolidated classes)
ZCTA_DIFF_SUMMARY_FILE = './output/diff_summary_data.json' # file for summary data for cropland data change each year

def main():
  """
  This is the main function that executes the workflow for processing and analyzing cropland data.

  It performs the following steps:
  1. Clips the cropscape data to the area of interest.
  2. Consolidates cropland classes by reclassifying cropland data layer rasters.
  3. Generates summary data for each cropland data year.
  4. Saves the summary data to a JSON file.
  5. Computes the difference between adjacent year consolidated rasters for select pixel classes.
  6. Summarizes the difference for each ZCTA shape.
  7. Saves the consolidated rasters summary data to a JSON file.
  8. Prints the elapsed time of the entire process.

  Note: The specific details of each step are not provided in this docstring. Please refer to the code comments
  for more information on each step.

  Args:
    None

  Returns:
    None
  """
  
  start_time = time.time()

  console = rich.console.Console()
  status = console.status('[bold green]Working...[/bold green]')
  status.start()

  # limit to our area of interest by clipping first, which will also make subsequent steps faster
  status.update('Clipping cropscape data to area of interest...')
  clip_cropscape_to_area_of_interest(CROPSCAPE_INPUT_FOLDER, AREA_OF_INTEREST_SHAPEFILE, CLIPPED_RASTERS_FOLDER)
  console.log('Cropscape data clipped to area of interest')

  # Consolidate cropland data by reclassifying cropland data layer rasters
  # such that cropland is a single pixel value. Other pixel types are also
  # grouped together (e.g, all forest, all develolped land, all cropland, etc.).
  status.update('Consolidating cropland classes...')
  reclassify_rasters(CLIPPED_RASTERS_FOLDER[1:], CONSOLIDATED_RASTERS_FOLDER[1:], reclass_spec)
  console.log('Cropland classess consolidated')

  # create a list containing the paths to all consilidated rasters
  # so we can easily loop through them later
  consolidated_rasters_list = sorted([str for str in os.listdir(CONSOLIDATED_RASTERS_FOLDER) if str.endswith("_30m_cdls.tif")])

  # create a list containing the paths to all zcta shapefiles
  # so we can easily loop through them later
  zcta_shapefiles_list = sorted([str for str in os.listdir(ZCTA_SHAPES_FOLDER) if str.endswith('.shp')])

  # generate summary data for each cropland data year
  # and store it in the `summary_data` list
  status.update('Generating summary data for each cropland data year...')
  summary_data = []
  for (index, filename) in enumerate(consolidated_rasters_list):
    file_root, _, year = get_raster_info_from_path(f'{CONSOLIDATED_RASTERS_FOLDER}/{filename}')
    
    for (zcta_index, zcta_filename) in enumerate(zcta_shapefiles_list):
      _, _, zcta_year, zcta_attr = get_zcta_info_from_path(f'{ZCTA_SHAPES_FOLDER}/{zcta_filename}')
      
      summary_data.append({
        'cropland_year': year,
        'zcta_year': zcta_year,
        'data': summarize_raster(
          f'{file_root}.tif',
          f'{file_root}.json',
          f'{ZCTA_SHAPES_FOLDER}/{zcta_filename}',
          zcta_attr,
          f'{ZCTA_CLIPPED_AND_SUMMARY_STATS_FOLDER}/{zcta_year}',
          status=status,
          status_prefix=f'[{year}|{zcta_year}] '
        ) 
      })
      console.log(f'Summary data for {year} and {zcta_year} saved')

  # save the `summary_data` list to JSON file
  with open(ZCTA_SUMMARY_FILE, "w") as file:
    json.dump(summary_data, file, indent=2) 
    console.log('Summary data saved to ./output/summary_data.json')

  # compute the difference between adjacent year consolidated rasters
  # for select pixel classes and summarize the difference for each ZCTA shape
  consolidated_rasters_summary_data = []
  for (index, filename) in enumerate(consolidated_rasters_list):
    if index > 0:
      file_root, _, year = get_raster_info_from_path(f'{CONSOLIDATED_RASTERS_FOLDER}/{filename}')
      last_year = int(consolidated_rasters_list[index - 1][0:4])
      
      file_last_year = f'{CONSOLIDATED_RASTERS_FOLDER}/{last_year}_30m_cdls.tif'
      file_this_year = f'{CONSOLIDATED_RASTERS_FOLDER}/{year}_30m_cdls.tif'
      file_diff_root = f'./output/diff/{last_year}_{year}'
      
      status.update(f'Computing difference between {last_year} and {year}...')
      compute_raster_class_difference(
        file_last_year,
        file_this_year,
        {
          1: { 'color': (255, 0, 0), 'name': 'crops to developed', 'from': [1], 'to': [10, 11, 12, 13, 14] },
          2: { 'color': (0, 255, 0), 'name': 'crops to forest', 'from': [1], 'to': [4] },
          3: { 'color': (255, 255, 0), 'name': 'crops to idle', 'from': [1], 'to': [2] },
          4: { 'color': (0, 0, 255), 'name': 'crops to grassland, shrubland, barren, or wetlands', 'from': [1], 'to': [3, 5, 6, 21, 22] },
          5: { 'color': (20, 20, 20), 'name': 'crops to crops', 'from': [1], 'to': [1] },
          10: { 'color': (20, 20, 20), 'name': 'other to crops', 'from': [2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 21, 22], 'to': [1] },
        },
        f'{file_diff_root}.tiff',
      )
      console.log(f'Difference between {last_year} and {year} computed')
      
      status.update(f'Summarizing difference between {last_year} and {year} (may take a while)...')
      for (zcta_index, zcta_filename) in enumerate(zcta_shapefiles_list):
        _, _, zcta_year, zcta_attr = get_zcta_info_from_path(f'{ZCTA_SHAPES_FOLDER}/{zcta_filename}')
        
        consolidated_rasters_summary_data.append({
          'cropland_year_start': int(last_year),
          'cropland_year_end': int(year),
          'zcta_year': zcta_year,
          'data': summarize_raster(
            f'{file_diff_root}.tiff',
            f'{file_root}.json',
            f'{ZCTA_SHAPES_FOLDER}/{zcta_filename}',
            zcta_attr,
            f'{ZCTA_CLIPPED_AND_SUMMARY_STATS_FOLDER}/diff/{zcta_year}',
            status=status,
            status_prefix=f'[{last_year}-{year}|zcta{zcta_year}] '
          ) 
        })
      console.log(f'Difference between {last_year} and {year} summarized')
      
  # save the `consolidated_rasters_summary_data` list to JSON file
  with open(ZCTA_DIFF_SUMMARY_FILE, "w") as file:
    json.dump(consolidated_rasters_summary_data, file, indent=2) 
    console.log('Consolidated rasters summary data saved to ./output/consolidated_rasters_summary_data.json')

  status.stop()
  end_time = time.time()

  console.log(f'Elapsed time: {end_time - start_time} seconds')

reclass_spec: PixelRemapSpecs = {
  0: { 'color': (0, 0, 0), 'name': 'background', 'original': [0] },
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

diff_spec: PixelDiffSpecs = {
  1: { 'color': (255, 0, 0), 'name': 'crops to developed', 'from': [1], 'to': [10, 11, 12, 13, 14] },
  2: { 'color': (0, 255, 0), 'name': 'crops to forest', 'from': [1], 'to': [4] },
  3: { 'color': (255, 255, 0), 'name': 'crops to idle', 'from': [1], 'to': [2] },
  4: { 'color': (0, 0, 255), 'name': 'crops to grassland, shrubland, barren, or wetlands', 'from': [1], 'to': [3, 5, 6, 21, 22] },
  5: { 'color': (20, 20, 20), 'name': 'crops to crops', 'from': [1], 'to': [1] },
  10: { 'color': (20, 20, 20), 'name': 'other to crops', 'from': [2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 21, 22], 'to': [1] },
}

def get_raster_info_from_path(path: str) -> tuple[str, str, int]:
  """
  Retrieves information about a raster file from the given path.

  Args:
    path (str): The path to the raster file.

  Returns:
    tuple: A tuple containing the following information:
      - raster_file_path (str): The full path to the raster file.
      - raster_file_root (str): The root name of the raster file without the extension.
      - raster_file_ext (str): The extension of the raster file.
      - raster_year (int): The year extracted from the raster file name.
  """
  raster_file_root, raster_file_ext = os.path.splitext(path)
  raster_year = int(raster_file_root[0:4])
  return (raster_file_root, raster_file_ext, raster_year)

def get_zcta_info_from_path(path: str) -> tuple[str, str, int, str | None]:
  """
  Retrieves information about a ZCTA file from the given path.

  Args:
    path (str): The path to the ZCTA file.

  Returns:
    tuple: A tuple containing the following information:
      - zcta_file_path (str): The full path to the ZCTA file.
      - zcta_file_root (str): The root name of the ZCTA file without the extension.
      - zcta_file_ext (str): The extension of the ZCTA file.
      - zcta_year (int): The year extracted from the ZCTA file name.
      - id_key (str): The attribute name to use as the identifier for each shape an NHGIS ZCTA shapefile.
  """
  zcta_file_root, zcta_file_ext = os.path.splitext(path)
  zcta_year = int(zcta_file_root[-4:])
  
  # this is shapefile column/attribute name to use as the identifier
  # for each shape that is being summarized for a ZCTA year
  # (for shapefiles from NHGIS)
  id_key = 'ZCTA5CE10' if zcta_year >= 2010 and zcta_year < 2020 else 'ZCTA5CE20' if zcta_year > 2020 else None

  return (zcta_file_root, zcta_file_ext, zcta_year, id_key)
