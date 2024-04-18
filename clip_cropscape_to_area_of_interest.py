import os
import geopandas
import rasterio
import rasterio.mask
import rasterio.warp
from shapely.geometry import shape

def clip_cropscape_to_area_of_interest(input_folder= '/input', clip_shape= '/input/area_of_interest.shp', output_folder= '/output'):
  """
  Clips the cropscape rasters in the input folder to the area of interest defined by the clip shapefile.
  
  Args:
    input_folder (str): Path to the folder containing the cropscape rasters.
    clip_shape (str): Path to the shapefile defining the area of interest.
    output_folder (str): Path to the folder where the clipped rasters will be saved.
  """
  dir_path = os.path.dirname(os.path.realpath(__file__))

  # create output folder
  if (not os.path.isdir(dir_path + output_folder)): 
    print('creating output folder...')
    os.mkdir(dir_path + output_folder)
    print('  ☑ Done')
    
  # create clipped cropscape rasters for every raster in the input folder
  for folder in sorted(os.listdir(dir_path + input_folder)):
    folder_path = dir_path + input_folder + '/' + folder
    if os.path.isdir(folder_path):
      for filename in sorted(os.listdir(folder_path)):
        file_path = folder_path + '/' + filename
        if filename.endswith("_30m_cdls.tif"):
          print(f'prcoessing {filename}...')
          year = filename[0:4]
          
          # open the raster and lock it in the filesystem while working on it
          raster = rasterio.open(file_path)
          
          # read the clip shapefile (.shp)
          clip_shp_original = geopandas.read_file(dir_path + clip_shape)

          # reproject the clip shape to match the raster projection
          # because rasterio requires matching projections for masking (clipping)
          print('  ...matching projections...')
          reprojection_geometry = rasterio.warp.transform_geom(
            src_crs=clip_shp_original.crs,
            dst_crs=raster.crs,
            geom=clip_shp_original.geometry.values,
          )
          clip_shp_reprojected = clip_shp_original.set_geometry(
              [shape(geom) for geom in reprojection_geometry],
              crs=raster.crs,
          )
          
          # clip raster to shapefile and rewrite output metadata
          print('  ...clipping...')
          out_image, out_transform = rasterio.mask.mask(raster, clip_shp_reprojected.geometry.values, crop=True)
          out_meta = raster.meta.copy()
          out_meta.update({ 
                            "driver": "GTiff",
                            "height": out_image.shape[1],
                            "width": out_image.shape[2],
                            "transform": out_transform,
                            "nodata": 0
                          })
          
          # export the clipped raster with same colormap as the source raster
          print('  ...exporting...')
          with rasterio.open(f'{dir_path}{output_folder}/{year}_30m_cdls.tif', "w", **out_meta) as dest:
            dest.write(out_image)
            dest.write_colormap(1, raster.colormap(1))

          # remove the lock on the raster
          raster.close()
          print('  ☑ Done')

  print('🏁 Finished')