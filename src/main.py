import argparse
import math
import os
import shutil
import sys
from contextlib import redirect_stdout

import fiona
import geopandas
from alive_progress import alive_bar, config_handler

from apply_cdl_data_to_parcels import apply_cdl_data_to_parcels
from filter_spatial_within import filter_spatial_within
from reclassify_raster import PixelRemapSpecs
from regrid_parcels_gdb_to_shp import geodatabases_to_geopackage

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

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Read a single parcel feature layer from an ESRI geodatabase, split it into chunks, calculate cropland data layer pixel coverage for each parcel, and the save to a GeoPackage.")
  parser.add_argument('--gdb_path', required=True, type=str, help="Path to the ESRI geodatabase.")
  parser.add_argument('--layer_name', required=True, type=str, help="Name of the feature layer.")
  parser.add_argument('--id_key', required=True, type=str, help="Column name of the unique identifier for the parcels. Will be truncated to 10 characters.")
  parser.add_argument('--output_gpkg', required=True, type=str, help="Path to the output GeoPackage.")
  parser.add_argument('--chunk_size', type=int, default=50000, help="Number of features per chunk (default is 1000).")
  parser.add_argument('--filter_layer_path', type=str, help="The file path to a shapefile to filter out features. The filter is a spatial within. Can be inverted with --invert-filter.")
  parser.add_argument('--cdls_folder_path', type=str, help="Path to folder containing the folders for each year of the Cropland Data Layer named with the format 'YYYY_30m_cdls'.")
  parser.add_argument('--cdls_aoi_shp_path', type=str, help="Path to a shapefile specifying the area of interest for the Cropland Data Layers. They will be cropped to the extent of this shapefile.")
  parser.add_argument('--invert-filter', type=bool, help="Invert the filter condition.")
  parser.add_argument('--summary_output_folder_path', type=str, default='./output', help="Folder to save the summary data.")

  args = parser.parse_args()
    
  output_logfile = f'./logging/{args.layer_name}.log'
  if (not os.path.isdir(f'./logging')): os.makedirs(f'./logging')
  with open(output_logfile, 'w') as logfile:
    sys.stdout.flush()
    
    hp = math.floor(len(output_logfile) / 2)
    
    print(f'')
    print(f'┌───────────────── \033[37;43mLogging output to {output_logfile}\033[0m ─────────────────┐')
    print(f'│                                                     {" " * len(output_logfile)} │')
    print(f'│ Run \033[93;1mtail -F {output_logfile}\033[0m in a separate terminal to view progress │')
    print(f'│                                                     {" " * len(output_logfile)} │')
    print(f'│ {" " * hp}                 \033[93;2mCTRL + C\033[0m\033[2m to cancel\033[0m                 {" " * hp} │')
    print(f'└─────────────────────────────────────────────────────{"─" * len(output_logfile)}─┘')
    # print(f'Run \033[93;1mtail -F {output_logfile}' + " 2>&1 | perl -ne 'if (/file truncated/) {system 'clear'} else {print}'} | grep " + '"ERROR"' + "\033[0m in a separate terminal to view progress (clear when restarted)")
    with redirect_stdout(logfile):
      try:
        max_cols = 120
        config_handler.set_global(force_tty=True, max_cols=max_cols)
        config_handler.set_global(title_length=32)
                
        # print all arguments in args
        print(f'\n\n\n\n\n\n\n\n\n\nArguments:')
        for arg in vars(args):
          print(f'  {arg}: {getattr(args, arg)}')
                
        print(f'\n{"─" * max_cols}\nChunking {args.layer_name} in {args.gdb_path} into {args.chunk_size}-feature chunks...')
        
        # remove working and ouput folders/paths if they exist
        if (os.path.isdir('./working')): shutil.rmtree('./working')
        if (os.path.isdir(args.summary_output_folder_path)):
          for item in os.listdir(args.summary_output_folder_path):
            if (os.path.isfile(os.path.join(args.summary_output_folder_path, item))): os.remove(os.path.join(args.summary_output_folder_path, item))
            else: shutil.rmtree(os.path.join(args.summary_output_folder_path, item))
        if (os.path.exists(args.output_gpkg)): os.remove(args.output_gpkg)
              
        # read the feature layer from the geodatabase
        with alive_bar(title='Reading feature layer from geodatabase', monitor=False) as bar:
          gdb_name = os.path.basename(args.gdb_path)
          gdf = geopandas.read_file(args.gdb_path, layer=args.layer_name, engine='pyogrio', use_arrow=True, columns=[args.id_key, 'lat', 'lon'])
          
        # split the feature layer into chunks
        with alive_bar(title='Chunking feature layer', total=math.ceil(len(gdf) / int(args.chunk_size))) as bar:
          chunks = []
          for i in range(0, len(gdf), args.chunk_size):
            chunks.append(gdf.iloc[i:i + args.chunk_size])
            bar()
              
        # save each chunk into a different layer in the GeoPackage
        with alive_bar(title='Saving chunks to GeoPackage', total=len(chunks)) as bar:
          chunked_gpkg_path = f'./working/{gdb_name}/{args.layer_name}__chunked.gpkg'
          filtered_chunked_gpkg_path = f'./working/{gdb_name}/{args.layer_name}__chunked__filtered.gpkg'
          
          # create the folder for the GeoPackage
          if (not os.path.isdir(os.path.dirname(chunked_gpkg_path))):
            os.makedirs(os.path.dirname(chunked_gpkg_path))
          
          # save each chunk into a different layer in the GeoPackage
          for i, chunk in enumerate(chunks):
            layer_chunk = f'layer_{i + 1}'
            chunk.to_file(chunked_gpkg_path, layer=layer_chunk, driver='GPKG', append=True)
            bar()
                      
        # create a new geopackage without urban area parcels
        if (args.filter_layer_path):
          filter_spatial_within(
            input_layer_path=chunked_gpkg_path,
            filter_layer_path=args.filter_layer_path,
            output_layer_path=filtered_chunked_gpkg_path,
            invert=args.invert_filter,
            loop_print='\n' + '─' * max_cols + '\nFiltering (spatial within) for chunk "{chunk_name}" ({count}/{total})...'
          )
                
        # create a list of the chunked layers by reading the GeoPackage
        gpkg_path = filtered_chunked_gpkg_path if args.filter_layer_path else chunked_gpkg_path
        chunk_names = fiona.listlayers(gpkg_path)
        
        # create temporary shapefile versions of each chunk since `apply_cdl_data_to_parcels` requires shapefiles
        print(f'\n{"─" * max_cols}')
        with alive_bar(len(chunk_names), title='Saving chunks to shapefiles') as bar:
          for chunk_name in chunk_names:
            chunk_gdf = geopandas.read_file(gpkg_path, layer=chunk_name, engine='pyogrio', use_arrow=True)
            chunk_gdf.to_file(f'./working/{gdb_name}/{args.layer_name}__{chunk_name}.shp')
            bar()
        
        # for each chunk, process the feature layer
        for index, chunk_name in enumerate(chunk_names):
          print(f'\n{"─" * max_cols}\nProcessing chunk "{chunk_name}" ({index + 1}/{len(chunk_names)})...')
          
          
          apply_cdl_data_to_parcels(
            cropscape_input_folder=args.cdls_folder_path, # folder containing cropland data layer rasters folders
            area_of_interest_shapefile=args.cdls_aoi_shp_path, # shapefile defining area of interest
            clipped_rasters_folder='./working/clipped', # folder for rasters clipped to area of interest
            consolidated_rasters_folder='./working/consolidated', # folder for consolidated cropland data layer rasters
            reclass_spec=reclass_spec,
            parcels_shp_path=f'./working/{gdb_name}/{args.layer_name}__{chunk_name}.shp',
            id_key=args.id_key[:10],
            parcels_summary_file=f'{args.summary_output_folder_path}/{chunk_name}__summary_data.json',
            clipped_parcels_rasters_folder='./working/clipped_parcels_rasters',
            parcels_trajectories_file=f'{args.summary_output_folder_path}/{chunk_name}__trajectories.json',
            parcels_gpkg_output_path=f'./working/{gdb_name}/{args.layer_name}__{chunk_name}__output.gpkg'
          )
          #args.output_gpkg
        
        # merge all the chunked layers into a single layer
        merged_counts_gdf = geopandas.GeoDataFrame()
        merged_trajectories_gdf = geopandas.GeoDataFrame()
        with alive_bar(2 * len(chunk_names), title='Merging chunked layers') as bar:
          for chunk_name in chunk_names:
            chunk_counts_gdf = geopandas.read_file(f'./working/{gdb_name}/{args.layer_name}__{chunk_name}__output.gpkg', layer='Parcels with CDL counts', engine='pyogrio', use_arrow=True)
            merged_counts_gdf = pandas.concat([merged_counts_gdf, chunk_counts_gdf], ignore_index=True)
            bar()
            chunk_trajectories_gdf = geopandas.read_file(f'./working/{gdb_name}/{args.layer_name}__{chunk_name}__output.gpkg', layer='Parcels with CDL pixel trajectories', engine='pyogrio', use_arrow=True)
            merged_trajectories_gdf = pandas.concat([merged_trajectories_gdf, chunk_trajectories_gdf], ignore_index=True)
            bar()
          
        # save merged layers to the output GeoPackage
        with alive_bar(2, title='Saving merged layers', monitor=False) as bar:
          merged_counts_gdf.to_file(args.output_gpkg, layer='Parcels with CDL counts', driver='GPKG')
          bar()
          merged_trajectories_gdf.to_file(args.output_gpkg, layer='Parcels with CDL pixel trajectories', driver='GPKG')
          bar()
          
        print('DONE')

      except Exception as e:
        print(e)
        sys.exit(f'There was an error. Please check the log file at {output_logfile} for more information.')
