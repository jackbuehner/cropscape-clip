import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import rasterio
import rasterio.mask
import rasterio.warp
from alive_progress import alive_bar

from clip_raster import clip_raster


def __clip_and_save_raster(file_path: str, clip_shape_path: str, out_file_path: str) -> None:
  out_image, out_transform, out_meta, out_colormap = clip_raster(file_path, clip_shape_path)
  with rasterio.open(out_file_path, "w", **out_meta) as dest:
    dest.write(out_image[0], 1)
    dest.write_colormap(1, out_colormap)

def clip_cropscape_to_area_of_interest(input_folder: str = './input', clip_shape_path: str = './input/area_of_interest.shp', output_folder: str = './output') -> None:
  """
  Clips the cropscape rasters in the input folder to the area of interest defined by the clip shapefile.
  
  Args:
    input_folder (str): Path to the folder containing the cropscape rasters.
    clip_shape (str): Path to the shapefile defining the area of interest.
    output_folder (str): Path to the folder where the clipped rasters will be saved.
  """
  # make the output folder if it does not exist
  if (not os.path.isdir(output_folder)): 
    os.makedirs(output_folder)
  
  # create a list of files to process
  files_to_process = []
  for folder in sorted(os.listdir(input_folder)):
    folder_path = input_folder + '/' + folder
    if os.path.isdir(folder_path):
      for filename in sorted(os.listdir(folder_path)):
        file_path = folder_path + '/' + filename
        if filename.endswith("_30m_cdls.tif"):
          files_to_process.append((filename, file_path))
                    
  # clip and save the files to the output folder using multiprocessing
  with alive_bar(len(files_to_process), title='Clipping to AOI') as bar, ProcessPoolExecutor() as executor:
    futures = []
    
    # queue each function to be executed
    for filename, file_path in files_to_process:
      out_file_path = f'{output_folder}/{filename}'
      future = executor.submit(__clip_and_save_raster, file_path, clip_shape_path, out_file_path)
      futures.append(future)
    
    # increment the progress bar as each future completes
    for future in as_completed(futures):
      bar()
    
  
        