import os

import rasterio
import rasterio.mask
import rasterio.warp
from rich.status import Status

from clip_raster import clip_raster


def clip_cropscape_to_area_of_interest(input_folder: str = './input', clip_shape_path: str = './input/area_of_interest.shp', output_folder: str = './output', *, status: Status | None = None, status_prefix: str = '') -> None:
  """
  Clips the cropscape rasters in the input folder to the area of interest defined by the clip shapefile.
  
  Args:
    input_folder (str): Path to the folder containing the cropscape rasters.
    clip_shape (str): Path to the shapefile defining the area of interest.
    output_folder (str): Path to the folder where the clipped rasters will be saved.
  """
  for folder in sorted(os.listdir(input_folder)):
    folder_path = input_folder + '/' + folder
    if os.path.isdir(folder_path):
      for filename in sorted(os.listdir(folder_path)):
        file_path = folder_path + '/' + filename
        if filename.endswith("_30m_cdls.tif"):
          out_image, out_transform, out_meta, out_colormap = clip_raster(file_path, clip_shape_path, status=status, status_prefix=f'{status_prefix}[{filename}] ')
          out_file_path = f'{output_folder}/{filename}'
          with rasterio.open(out_file_path, "w", **out_meta) as dest:
            if status: status.update(f'{status_prefix}[{filename}] Saving clipped raster to {out_file_path}...')
            dest.write(out_image[0], 1)
            dest.write_colormap(1, out_colormap)
            if status: status.console.log(f'{status_prefix}[{filename}] Clipped raster saved to {out_file_path}')