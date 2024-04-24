import copy
import os
from typing import Any, TypedDict

import numpy
import numpy.typing
import rasterio

PixelRemapSpec = TypedDict('PixelRemapSpec', {'color': tuple[int, int, int], 'name': str, 'original': list[int]})
PixelRemapSpecs = dict[int, PixelRemapSpec]

def reclassify_raster(input_raster_path: str, output_raster_path: str, remap: PixelRemapSpecs) -> numpy.typing.NDArray[Any]:
    """
    Reclassify raster data based on the provided reclassification specification.
    
    Args:
      input_raster_path (str): the path to the input raster file
      output_raster_path (str): the path to the output raster file
      remap (PixelRemapSpecs): the reclassification specification
      
    Type Hints:
      PixelRemapSpec: TypedDict('PixelRemapSpec', {'color': tuple[int, int, int], 'name': str, 'original': list[int]})
      PixelRemapSpecs: dict[int, PixelRemapSpec]
      
    Returns:
      the reclassified raster data
    """
    # open the raster and lock it in the filesystem while working on it
    raster = rasterio.open(input_raster_path)
    band1: numpy.typing.NDArray[Any] = raster.read(1)
    
    # reclassify based on the provided specifications
    reclassified = copy.deepcopy(band1)
    for key, value in remap.items():
      # reclass to negative number so that it will not be changed to a value
      # that is used in a different remap specification
      # (we will covert back to positive values once we are done with the loop)
      reclassified = numpy.where(numpy.isin(reclassified, value['original']), key * -1, reclassified)
      
    # convert the negative values back to positive values
    reclassified = numpy.absolute(reclassified)
    
    # calculate the colormap based on the remap specification
    colormap = {key: value['color'] for key, value in remap.items()}
    
    # export the extracted band pixel values
    # with the extracted band as black pixels and the rest as transparent white pixels
    out_profile = raster.profile.copy()
    out_profile.update(nodata=0)
    with rasterio.open(output_raster_path, "w", **out_profile) as dest:
      dest.write(reclassified, 1)
      dest.write_colormap(1, colormap)

    # remove the lock on the raster
    raster.close()
    
    return reclassified
 
def reclassify_rasters(input_folder: str, output_folder: str, remap: PixelRemapSpecs) -> None:
  '''
  Relcaassify all rasters in the input folder based on the provided remap specification.
  
  Args:
    input_folder (str): the path to the input folder
    output_folder (str): the path to the output folder
    remap (PixelRemapSpecs): the reclassification specification
    
  Type Hints:
    PixelRemapSpec: TypedDict('PixelRemapSpec', {'color': tuple[int, int, int], 'name': str, 'original': list[int]})
    PixelRemapSpecs: dict[int, PixelRemapSpec]
    
  Returns:
    None
  '''
  dir_path = os.path.dirname(os.path.realpath(__file__))

  # create output folder
  if (not os.path.isdir(dir_path + output_folder)): 
    print('creating output folder...')
    os.makedirs(dir_path + output_folder)
    print('  ☑ Done')
    
  
  # process for every raster in the input folder
  for filename in sorted(os.listdir(dir_path + input_folder)):
    file_path = dir_path + input_folder + '/' + filename
    if filename.endswith("_30m_cdls.tif"):
      print(f'processing {filename}...')
      year = filename[0:4]
      
      reclassify_raster(file_path, f'{dir_path}{output_folder}/{year}_30m_cdls.tif', remap)
      
      print('  ☑ Done')

  print('🏁 Finished')
  