import json
import os
import shutil
import time

import numpy
import numpy.typing
import pandas
import rasterio
from rich import console as rc
from rich.status import Status

from compute_raster_class_difference import compute_raster_class_difference
from reclassify_raster import PixelRemapSpecs, reclassify_rasters

ZCTA_CLIPPED_AND_SUMMARY_STATS_FOLDER = './working/zcta' # folder for consolidated cropland data layer rasters clipped to each zcta shape and their summary data

def calculate_pixel_trajectories(raster_folder_path: str, reclass_spec: PixelRemapSpecs, output_trajectories_file: str | None = None, temp_folder_path: str | None = None, *, status: Status | None = None, status_prefix: str = '') ->  dict[str, int]:
  # create a list containing the paths to all consilidated rasters
  # so we can easily loop through them later
  consolidated_rasters_list = sorted([(f'{raster_folder_path}/{str}', int(str[0:4])) for str in os.listdir(raster_folder_path) if str.endswith(".tif") or str.endswith(".tiff")], key=lambda x: x[1])

  # for each class, create boolean rasters that only indicate 1 or 0
  for class_num in reclass_spec:
    if (class_num == 0): continue
    spec = reclass_spec[class_num]
    
    if status: status.update(f'{status_prefix}Creating boolean rasters for {spec["name"]}...')
    
    # create rasters from each consolidated raster that only indicates 1 or 0
    # for a single pixel class (e.g., 1 = developed, 0 = not developed)
    reclassify_rasters(
      raster_folder_path,
      f'{temp_folder_path or "./TEMPORARY"}/boolean_class/{class_num}',
      {
        1: { 'color': (85, 237, 252), 'name': spec["name"], 'original': [class_num] },
        0: { 'color': (0, 0, 0), 'name': f'not {spec["name"]}', 'original': list(range(1, class_num)) + list(range(class_num + 1, 256)) }
      },
      False
    )

  # for each class, calculate the difference between the boolean rasters
  # for each year such that we have rasters indicating if a pixel became
  # that class, did not change, or lost that class
  diff_dict: dict[int, dict[str, numpy.typing.NDArray[numpy.int16]]] = {} # class: list of ndarray list representations of the difference between rasters
  for class_num in reclass_spec:
    if (class_num == 0): continue
    spec = reclass_spec[class_num]
    
    rasters_list = sorted([(f'{temp_folder_path or "./TEMPORARY"}/boolean_class/{class_num}/{str}', int(str[0:4])) for str in os.listdir(raster_folder_path) if str.endswith(".tif") or str.endswith('.tiff')], key=lambda x: x[1])
    
    for (index, (file_path, year)) in enumerate(rasters_list):
      # for the first year, just provide the array directly from
      # the raster since there is no previous year to compare to
      if index == 0:
        if status: status.update(f'{status_prefix}Parsing {file_path}...')
        with rasterio.open(file_path) as src:
          first_year_array = src.read(1)
          diff_dict[class_num] = {}
          diff_dict[class_num][f'{year}'] = first_year_array
        continue
      
      
      prev_file_path, prev_year = rasters_list[index - 1]
      if status: status.update(f'{status_prefix}Computing difference for {spec["name"]} between {prev_year} and {year}...')
      
      # calculate the difference between the rasters for the current and last year
      # so that we can see what changed between the two years
      # (e.g, for crops, 1 = new cropland, 254 = lost croplond)
      array = compute_raster_class_difference(
        prev_file_path,
        file_path,
        { 
          1: { 'color': (67, 96, 236), 'name': 'new', 'from': [0], 'to': [1] }, 
          254: { 'color': (255, 51, 50), 'name': 'new', 'from': [1], 'to': [0] }
        },
        # f'{temp_folder_path or "./TEMPORARY"}/diff/{class_num}/{prev_year}_{year}.tiff',
      )
      diff_dict[class_num][f'{prev_year}_{year}'] = array
  
  # create a multidemensial array that contains the change for each class each year for each pixel
  # rows x columns x depth = class x year x pixel
  if status: status.update(f'{status_prefix}Constructing pixel array structure...')
  years_count = len(consolidated_rasters_list) 
  classes_count = len(reclass_spec)
  pixels_count = list(list(diff_dict.values())[0].values())[0].size
  array = numpy.zeros((pixels_count, classes_count, years_count), dtype=numpy.int32)

  # loop through the diff_dict and place the pixel values into the multidimensional array
  for class_index, (class_num, class_dict) in enumerate(diff_dict.items()):
    for year_range_index, (year_range, class_year_diff_array) in enumerate(class_dict.items()):
      if status: status.update(f'{status_prefix}Assigning pixels to array for year {year_range} and class {reclass_spec[class_num]["name"]}...')
      counter = 0 # this increments for each pixel so that each pixel is placed into a different dimension in the array
      for row in class_year_diff_array:
        for pixel in row:
          try:
            # z, y, x coordinates
            # pixel, class, year coordinates
            array[counter][class_index][year_range_index] = numpy.int32(pixel) 
            pass
          except Exception as e:
            if status: status.console.log(f'{status_prefix}{e}')
            pass
          counter += 1

  years_in_order = list(list(diff_dict.values())[0].keys())
  class_names_in_order = [reclass_spec[class_number]['name'] for class_number in diff_dict.keys()]

  # identify the pixel trajectories
  if status: status.update(f'{status_prefix}Identifying pixel trajectories...')
  pixel_trajectories_counts: dict[str, int] = {}
  for z in range(len(array)):
    # transpose the array so that we can loop through the years in order
    transposed = array[z].transpose()
    
    # loop through each year and find the new class for each pixel if it exists
    trajectory = ''
    for year_range_index, year_range in enumerate(years_in_order):
      for class_index, class_name in enumerate(class_names_in_order):
        pixel_value = transposed[year_range_index][class_index]
        if pixel_value == 1:
          string = f'{class_name} → '
          if trajectory.endswith(string): break
          trajectory += string
          break
        
    if trajectory:
      pixel_trajectories_counts[trajectory] = pixel_trajectories_counts.get(trajectory, 0) + 1
    
  if status: status.update(f'{status_prefix}Saving pixel trajectories...')
  clean_trajectory_counts = { key[:-3]: value for key, value in pixel_trajectories_counts.items() }
  sorted_trajectory_counts = dict(sorted(clean_trajectory_counts.items(), key=lambda item: item[0], reverse=True))
  if output_trajectories_file:
    with open(output_trajectories_file, 'w') as file:
      json.dump(sorted_trajectory_counts, file, indent=2, ensure_ascii=False)
    
  shutil.rmtree(temp_folder_path or './TEMPORARY')
  
  if status: status.console.log(f'{status_prefix}Finished computing pixel trajectories')
  return sorted_trajectory_counts

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

# console = rc.Console()
# status = console.status('[bold green]Working...[/bold green]')
# status.start()
# start_time = time.time()

# for zip_str in os.listdir(f'{ZCTA_CLIPPED_AND_SUMMARY_STATS_FOLDER}/2022'):
#   calculate_pixel_trajectories(
#     f'{ZCTA_CLIPPED_AND_SUMMARY_STATS_FOLDER}/2022/{zip_str}',
#     reclass_spec,
#     f'./output/trajectories_2022_{zip_str}.json',
#     status=status,
#     status_prefix=f'[{zip_str}] '
#   )

# status.console.log('Finished computing pixel trajectories')
# status.stop()
# end_time = time.time()

# console.log(f'Elapsed time: {end_time - start_time} seconds')
