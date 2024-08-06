import sys
from contextlib import redirect_stdout

from alive_progress import config_handler

from apply_cdl_data_to_parcels import apply_cdl_data_to_parcels
from filter_spatial_within import filter_spatial_within
from reclassify_raster import PixelRemapSpecs
from regrid_parcels_gdb_to_shp import geodatabases_to_geopackage

if __name__ == '__main__':
  output_logfile = 'log.txt'
  with open(output_logfile, 'w') as logfile:
    sys.stdout.flush()
    
    print(f'')
    print(f'┌───────────────── \033[37;43mLogging output to {output_logfile}\033[0m ─────────────────┐')
    print(f'│                                                             │')
    print(f'│ Run \033[93;1mtail -F {output_logfile}\033[0m in a separate terminal to view progress │')
    print(f'│                                                             │')
    print(f'│                     \033[93;2mCTRL + C\033[0m\033[2m to cancel\033[0m                      │')
    print(f'└─────────────────────────────────────────────────────────────┘')
    # print(f'Run \033[93;1mtail -F {output_logfile}' + " 2>&1 | perl -ne 'if (/file truncated/) {system 'clear'} else {print}'} | grep " + '"ERROR"' + "\033[0m in a separate terminal to view progress (clear when restarted)")
    with redirect_stdout(logfile):
      config_handler.set_global(force_tty=True, max_cols=120)
      config_handler.set_global(title_length=32)

      
      # combine all parcel feature classes into a single shapefile
      # print('Merging all parcel feature layers into a single geopackage...')
      geodatabases_to_geopackage(
        geodatabases_folder_path='./input/parcels',
        output_gpkg_path='./working/parcels_all.gpkg',
        columns_to_parse=['parcelnumb_no_formatting', 'lat', 'lon']
      )
      
      # create a new shapefile without urban area parcels
      print('Filtering out urban area parcels...')
      filter_spatial_within(
        input_layer_path='./working/parcels.shp',
        filter_layer_path='./input/urban_areas_2020_corrected/urban_areas_2020_corrected.shp',
        output_layer_path='./working/parcels_rural.shp',
        invert=True
      )
      
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
      
      # process cropland data with parcels
      apply_cdl_data_to_parcels(
        cropscape_input_folder='./input', # folder containing cropland data layer rasters folders
        area_of_interest_shapefile='./input/area_of_interest.shp', # shapefile defining area of interest
        clipped_rasters_folder='./working/clipped', # folder for rasters clipped to area of interest
        consolidated_rasters_folder='./working/consolidated', # folder for consolidated cropland data layer rasters
        reclass_spec=reclass_spec,
        id_key='parcelnumb', # truncated version of parcelnumb_no_formatting
        parcels_summary_file='./output/summary_data.json',
        clipped_parcels_rasters_folder='./working/clipped_parcels_sc_greenville_rural',
        parcels_trajectories_file='./output/trajectories.json',
        parcels_gpkg_output_path='./output/parcels.gpkg'
      )
