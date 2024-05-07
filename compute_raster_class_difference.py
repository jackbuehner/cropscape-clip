import os
from typing import Any, TypedDict

import numpy
import numpy.typing
import rasterio

PixelDiffSpec = TypedDict('PixelDiffSpec', { 'color': tuple[int, int, int], 'name': str, 'from': list[int], 'to': list[int] })
PixelDiffSpecs = dict[int, PixelDiffSpec]

def compute_raster_class_difference(input_from_raster: str, input_to_raster: str, remap: PixelDiffSpecs, output_diff_rast: str | None = None) -> numpy.typing.NDArray[numpy.int16]:
  '''
  Compute the difference between two input rasters based on a pixel difference specification.
  If there is no change, the resulting pixel value will be 0. Otherwise, the pixel value
  will be based on the reclassification specified in the remap dictionary.

  Parameters:
    input_from_raster (str): The path to the input "from" raster.
    input_to_raster (str): The path to the input "to" raster.
    output_diff_rast (str): The path to the output difference raster.
    remap (PixelDiffSpecs): A dictionary specifying the pixel difference values and their corresponding reclassified values.

  Raises:
    ValueError: If the two input rasters have different shapes.
    ValueError: If the remap dictionary contains a value for 0.

  Returns:
    numpy.typing.NDArray[numpy.int16]: The numpy array representing the reclassified difference raster.
  '''

  # make the output folder if it does not exist
  if output_diff_rast:
    dir_path = os.path.dirname(output_diff_rast)
    if (not os.path.isdir(dir_path)): 
      os.makedirs(dir_path)

  # Open the input rasters
  with rasterio.open(input_from_raster) as src_from, rasterio.open(input_to_raster) as src_to:
    # Read the raster data
    from_data = src_from.read(1)
    to_data = src_to.read(1)

    # require the two rasters to have the same shape
    if from_data.shape != to_data.shape:
      raise ValueError('The two rasters must have the same shape.')
    
    # do not allow zeros for the output pixel value spec because it is used to represent no change
    if 0 in remap.keys():
      raise ValueError('The remap dictionary must not contain a value for 0.')

    # create an empty array that will represent the output raster
    # and fill it with the reclassified values based on the
    # pixel difference specification
    reclassified: numpy.typing.NDArray[Any] = numpy.zeros(from_data.shape, dtype=numpy.int16)
    for key, value in remap.items():
      reclassified = numpy.where((numpy.isin(from_data, value['from'])) & (numpy.isin(to_data, value['to'])), key, reclassified)

    # calculate the colormap based on the remap specification
    colormap = {key: value['color'] for key, value in remap.items()}
    colormap.update({ 0: (0, 0, 0) })

    # write the difference raster to the output file
    if output_diff_rast:
      with rasterio.open(output_diff_rast, 'w', **src_from.profile) as dest:
        dest.write(reclassified, 1)
        dest.write_colormap(1, colormap)
    
    # also return the numpy array representing the reclassified raster
    return reclassified
