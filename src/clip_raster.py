from typing import Any

import affine
import geopandas
import numpy
import pandas
import rasterio
import rasterio.features
from geopandas.geodataframe import GeoDataFrame
from rasterio.io import DatasetReader
from rich.status import Status
from shapely.geometry import shape
from rasterio.mask import mask


def clip_raster(raster: DatasetReader | str, clip_shape: GeoDataFrame | str, feature_indices: pandas.core.indexing._iLocIndexer | None = None, *, status: Status | None = None, status_prefix: str = '') -> tuple[numpy.ndarray[Any, numpy.dtype[numpy.int32]], affine.Affine, dict[Any, Any], dict[Any, Any]]:  
  '''
  Clips a raster to the extent of the first feature in a geodataframe.
  
  When providing a string path to a raster or shapefile, the function will
  open the file and close it when done.
  
  Args:
    raster (rasterio.io.DatasetReader | str): The raster to clip.
    clip_shape (geopandas.geodataframe.GeoDataFrame | str): The shape to clip to.
    feature_index: (pandas.core.indexing._iLocIndexer | None): The index of the feature to clip to. If None, all features will be used.
  '''
  
  # get the raster as a DatasetReader
  _raster: DatasetReader
  should_close_raster = False
  if status: status.update(f'{status_prefix}Reading input raster...')
  if isinstance(raster, str):
    if status: status.update(f'{status_prefix}Opening raster...')
    _raster = rasterio.open(raster)
    if status: status.console.log(f'{status_prefix}Raster opened from {raster}')
    should_close_raster = True # we will need to close it when we are done
  elif isinstance(raster, DatasetReader):
    _raster = raster
  else:
    raise ValueError('raster_path_or_dataset must be a string or a rasterio.io.DatasetReader')
  if status: status.update(f'{status_prefix}Input raster read')

  # get the clip shape as a GeoDataFrame with only a single feature
  clip_shp_original: GeoDataFrame
  if isinstance(clip_shape, str):
    if status: status.update(f'{status_prefix}Reading input feature layer from {clip_shape}...')
    # if a file path is provided, read the file to a geodatframe
    # and then create a GeoDataFrame with only the first feature
    geodataframe = geopandas.read_file(clip_shape)
    clip_shp_original = geopandas.GeoDataFrame(
      [geodataframe.iloc[0]],
      crs=geodataframe.crs,
      geometry=[geodataframe.iloc[0]['geometry']]
    )
    if status: status.console.log(f'{status_prefix}Feature layer opened from {clip_shape}')
  elif isinstance(clip_shape, GeoDataFrame):
    if (feature_indices is not None):
      if status: status.update(f'{status_prefix}Selecting specified input feature layer features...')
      # if a geodataframe is provided and feature_indices is provided,
      # create a new GeoDataFrame with only the features specified in
      # feature_indices from the provided GeoDataFrame
      try:
        clip_shp_original = geopandas.GeoDataFrame(
          [clip_shape.loc[feature_indices]], # we use loc because we are use the row label (which is marked as the index) instead of row integer index
          crs=clip_shape.crs,
          geometry=[clip_shape.loc[feature_indices]['geometry']]
        )
        if status: status.console.log(f'{status_prefix}Specified input feature layer features selected')
        pass
      except Exception as e:
        available_indices = clip_shape.index.tolist()
        raise IndexError(f'One or more of the specified feature indices is out of range (chose {feature_indices}, available ({available_indices}))') from e
      
    else:
      # otherwise, pass throguh the provided geodataframe
      if status: status.update(f'{status_prefix}Reading input feature layer GeoDataFrame...')
      clip_shp_original = clip_shape
      if status: status.console.log(f'{status_prefix}Input feature layer GeoDataFrame read')
  else:
    raise ValueError('clip_shape must be a string or a geopandas.geodataframe.GeoDataFrame')
    
  # reproject the clip shape to match the raster projection
  # because rasterio requires matching projections for masking (clipping)
  if status: status.update(f'{status_prefix}Reprojecting feature layer...')
  reprojection_geometry = rasterio.warp.transform_geom(
    src_crs=clip_shp_original.crs,
    dst_crs=_raster.crs,
    geom=clip_shp_original.geometry.values,
  )
  clip_shp_reprojected = clip_shp_original.set_geometry(
      [shape(geom) for geom in reprojection_geometry],
      crs=_raster.crs,
  )
  if status: status.console.log(f'{status_prefix}Feature layer reprojected')
    
  # clip the raster
  if status: status.update(f'{status_prefix}Clipping raster to feature layer...')
  out_image: numpy.ndarray[Any, numpy.dtype[numpy.int32]]
  out_transform: affine.Affine
  out_colormap: dict[Any, Any] = _raster.colormap(1)
  out_image, out_transform = mask(
    _raster,
    clip_shp_reprojected.geometry.values,
    crop=True
  )
  out_meta: dict[Any, Any] = _raster.meta.copy()
  out_meta.update({ 
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform,
                    "nodata": 0
                  })
  if status: status.console.log(f'{status_prefix}Raster clipped to feature layer')
  
  # close the raster if it was opened in this function
  if (should_close_raster):
    if status: status.update(f'{status_prefix}Closing raster...')
    _raster.close()
    if status: status.console.log(f'{status_prefix}Raster closed')
        
  return (out_image, out_transform, out_meta, out_colormap)
