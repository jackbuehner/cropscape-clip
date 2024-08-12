import copy
import os
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from time import sleep
from typing import Any, TypedDict

import numpy
import numpy.typing
import rasterio
from alive_progress import alive_bar

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
  with rasterio.open(input_raster_path) as raster:
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
  
    return reclassified

def reclassify_rasters(input_folder: str, output_folder: str, remap: PixelRemapSpecs, show_progress_bar: bool = True, use_multiprocessing: bool = True) -> None:
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
  # create output folder if it does not exist
  if (not os.path.isdir(output_folder)): 
    os.makedirs(output_folder)
    
  # create a list of files to process
  files_to_process = []
  for filename in sorted(os.listdir(input_folder)):
    file_path = input_folder + '/' + filename
    if filename.endswith(".tif") or filename.endswith(".tiff"):
      files_to_process.append((filename, file_path))
          
  # reclssify and save the rasters to the output folder using multiprocessing
  with alive_bar(len(files_to_process), title='Reclassifying rasters', disable=not show_progress_bar) as bar:
    
    if use_multiprocessing:
      with ProcessPoolExecutor() as executor:
        futures: list[Future[numpy.typing.NDArray[Any]]] = []
        
        # queue each function to be executed
        for filename, file_path in files_to_process:
          out_file_path = f'{output_folder}/{filename}'
          future = executor.submit(reclassify_raster, file_path, out_file_path, remap)
          futures.append(future)
          sleep(1)
            
        # increment the progress bar as each future completes
        for future in as_completed(futures):
          bar()
          
    else:
      for filename, file_path in files_to_process:
        out_file_path = f'{output_folder}/{filename}'
        reclassify_raster(file_path, out_file_path, remap)
        bar()
  