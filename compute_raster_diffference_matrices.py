import itertools
import shutil

import numpy
import pandas
from rich.status import Status

from compute_raster_class_difference import (PixelDiffSpec, PixelDiffSpecs,
                                             compute_raster_class_difference)
from reclassify_raster import PixelRemapSpecs
from summarize_raster import summarize_raster


def compute_raster_difference_matrices(rasters_list: list[tuple[str, int]], reclass_spec: PixelRemapSpecs, *, status: Status | None = None, status_prefix: str = '') -> tuple[list[str], list[pandas.DataFrame]]:
  """
  Compute the difference matrices for a list of rasters.

  Args:
    rasters_list (list[tuple[str, int]]): A list of tuples containing the file paths and corresponding years of the rasters.
    reclass_spec (PixelRemapSpecs): A dictionary specifying the remapping specifications for the pixel classes.
    status (Status | None, optional): A status object to track the progress of the computation. Defaults to None.
    status_prefix (str, optional): A prefix to be displayed in the status messages. Defaults to ''.

  Returns:
    tuple[list[str], list[pandas.DataFrame]]: A tuple containing the labels for the difference matrices and the difference matrices themselves.
  """
  if status: status.start()
  if status: status.console.log(f'{status_prefix}Started [cyan]compute_raster_difference_matrices[/cyan]') 
  
  # get the names of every raster pixel class
  classes = [(key, reclass_spec[key]['name']) for key in reclass_spec.keys()]
  classes_length = len(classes)

  # generate a dataframe that shows all pairs of classes and their positions
  pairs_list = [(f'{from_name} → {to_name}') for (_, from_name), (_, to_name) in itertools.product(classes, repeat=2)]
  pairs_array = numpy.array(pairs_list)
  pairs_matrix = numpy.matrix(pairs_array.reshape(classes_length, classes_length))
  pairs_dataframe = df=pandas.DataFrame(pairs_matrix, columns=classes, index=classes)

  # generate a diff spec for `compute_raster_class_difference` that contains
  # all possible pairs of classes and their reclassified values
  diff_specs: PixelDiffSpecs = {}
  for (index, ((from_id, from_name), (to_id, to_name))) in enumerate(itertools.product(classes, repeat=2)):
    this_diff_spec: PixelDiffSpec = {
      'color': (0, 0, 0),
      'name': f'{from_name} → {to_name}',
      'from': [from_id],
      'to': [to_id],
    } 
    diff_specs.update({index + 1: this_diff_spec})

  # loop through the rasters and calculate a difference proprtion matrix for each pair of years
  diff_matrices_labels: list[str] = ['Legend']
  diff_matrices: list[pandas.DataFrame] = [pairs_dataframe]
  for (index, (file_this_year, year)) in enumerate(rasters_list):
    # get data for the previous year
    file_last_year, last_year = next((t for t in rasters_list if t[1] == year - 1), (None, None))
    
    # cancel this iteration if there is no previous year available
    if file_last_year is None or last_year is None: continue
    
    # calculate the change in pixel classes between the previous year and the current year
    if status: status.update(f'{status_prefix}Computing difference between {last_year} and {year}...')
    diff_array = compute_raster_class_difference(
      file_last_year,
      file_this_year,
      diff_specs,
      f'./TEMPORARY/class_diffs/{last_year}_{year}.tiff'
    )
    if status: status.console.log(f'{status_prefix}Difference computed ({last_year} → {year})')
    
    # calculate summary metadata (pixel counts) for the rasters
    # so we can calculate proportions of change later
    from_metadata = summarize_raster(file_last_year)
    diff_metadata = summarize_raster(f'./TEMPORARY/class_diffs/{last_year}_{year}.tiff')
    if status: status.console.log(f'{status_prefix}Metadata summarized ({last_year} → {year})')
    
    # clean up temporary folder
    shutil.rmtree('./TEMPORARY')
    
    # build a list of proportions of change for every possible pair of classes
    diff_proportions_list = []
    for (from_id, from_name), (to_id, to_name) in itertools.product(classes, repeat=2):
      # get the spec, which contains the from and to pixel values
      diff_id, spec = next(((key, value) for (key, value) in diff_specs.items() if value['name'] == f'{from_name} → {to_name}'), (None, { 'from': [None] }))

      # get the counts and calculate a proportion
      from_count = from_metadata['pixel_counts'].get(spec['from'][0], 0)
      diff_count = diff_metadata['pixel_counts'].get(diff_id, 0)
      diff_proportion = 0 if from_count == 0 else diff_count / from_count
      diff_proportions_list.append(f'{diff_proportion:.4f}')
    
    # build a dataframe showing the proportion change/diff for each possible pair of classes
    diff_proportions_array = numpy.array(diff_proportions_list)
    diff_proportions_matrix = numpy.matrix(diff_proportions_array.reshape(classes_length, classes_length))
    diff_proportions_dataframe = df=pandas.DataFrame(diff_proportions_matrix, columns=classes, index=classes)
    diff_matrices_labels.append(f'{last_year} → {year}')
    diff_matrices.append(diff_proportions_dataframe)
    if status: status.console.log(f'{status_prefix}Difference proportions matrix calculated ({last_year} → {year})')
    
  return (diff_matrices_labels, diff_matrices)
