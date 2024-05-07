import json
import os

import rich

from clip_cropscape_to_area_of_interest import \
    clip_cropscape_to_area_of_interest
from reclassify_raster import reclassify_rasters
from summarize_raster import summarize_raster

console = rich.console.Console()
status = console.status('[bold green]Working...[/bold green]')
status.start()

# print(os.getcwd())

# limit to our area of interest by clipping first, which will also make subsequent steps faster
status.update('Clipping cropscape data to area of interest...')
clip_cropscape_to_area_of_interest('./input', './input/area_of_interest.shp', './working/clipped')
console.log('Cropscape data clipped to area of interest')

# Consolidate cropland data by reclassifying cropland data layer rasters
# such that cropland is a single pixel value. Other pixel types are also
# grouped together (e.g, all forest, all develolped land, all cropland, etc.).
status.update('Consolidating cropland classes...')
reclassify_rasters(
  '/working/clipped',
  '/output/consolidated',
  {
    0: { 'color': (0, 0, 0), 'name': 'background', 'original': [0] },
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
)
console.log('Cropland classess consolidated')

# generate summary data for each cropland data year
# and store it in the `summary_data` list
status.update('Generating summary data for each cropland data year...')
summary_data = []
for filename in sorted(os.listdir('./output/consolidated')):
  if filename.endswith("_30m_cdls.tif"):
    file_path = './output/consolidated' + '/' + filename
    file_root, file_ext = os.path.splitext(file_path)
    year = filename[0:4]
    
    for zcta_filename in sorted(os.listdir('./input/zcta')):
      if (zcta_filename.endswith('.shp')):
        zcta_file_path = './input/zcta' + '/' + zcta_filename
        zcta_file_root, zcta_file_ext = os.path.splitext(zcta_file_path)
        zcta_year = int(zcta_file_root[-4:])
        id_key = 'ZCTA5CE10' if zcta_year >= 2010 and zcta_year < 2020 else 'ZCTA5CE20' if zcta_year > 2020 else None
        summary_data.append({
          'cropland_year': int(year),
          'zcta_year': zcta_year,
          'data': summarize_raster(
            f'{file_root}.tif',
            f'{file_root}.json',
            f'./input/zcta/{zcta_filename}',
            id_key,
            f'./working/zcta/{zcta_year}',
            status=status,
            status_prefix=f'[{year}|{zcta_year}] '
          ) 
        })
        console.log(f'Summary data for {year} and {zcta_year} saved')
    
with open('./output/summary_data.json', "w") as file:
  json.dump(summary_data, file, indent=2) 
  console.log('Summary data saved to ./output/summary_data.json')

data_dictionary_str = '''
Data Dictionary: USDA National Agricultural Statistics Service, Cropland Data Layer 

Source: USDA National Agricultural Statistics Service 

The following is a cross reference list of the categorization codes and land covers. 
Note that not all land cover categories listed below will appear in an individual state. 

Raster 
Attribute Domain Values and Definitions: NO DATA, BACKGROUND 0 

Categorization Code   Land Cover 
          "0"       Background 

Raster 
Attribute Domain Values and Definitions: CROPS 1-60 

Categorization Code   Land Cover 
          "1"       Corn 
          "2"       Cotton 
          "3"       Rice 
          "4"       Sorghum 
          "5"       Soybeans 
          "6"       Sunflower 
         "10"       Peanuts 
         "11"       Tobacco 
         "12"       Sweet Corn 
         "13"       Pop or Orn Corn 
         "14"       Mint 
         "21"       Barley 
         "22"       Durum Wheat 
         "23"       Spring Wheat 
         "24"       Winter Wheat 
         "25"       Other Small Grains 
         "26"       Dbl Crop WinWht/Soybeans 
         "27"       Rye 
         "28"       Oats 
         "29"       Millet 
         "30"       Speltz 
         "31"       Canola 
         "32"       Flaxseed 
         "33"       Safflower 
         "34"       Rape Seed 
         "35"       Mustard 
         "36"       Alfalfa 
         "37"       Other Hay/Non Alfalfa 
         "38"       Camelina 
         "39"       Buckwheat 
         "41"       Sugarbeets 
         "42"       Dry Beans 
         "43"       Potatoes 
         "44"       Other Crops 
         "45"       Sugarcane 
         "46"       Sweet Potatoes 
         "47"       Misc Vegs & Fruits 
         "48"       Watermelons 
         "49"       Onions 
         "50"       Cucumbers 
         "51"       Chick Peas 
         "52"       Lentils 
         "53"       Peas 
         "54"       Tomatoes 
         "55"       Caneberries 
         "56"       Hops 
         "57"       Herbs 
         "58"       Clover/Wildflowers 
         "59"       Sod/Grass Seed 
         "60"       Switchgrass 

Raster 
Attribute Domain Values and Definitions: NON-CROP 61-65 

Categorization Code   Land Cover 
         "61"       Fallow/Idle Cropland 
         "62"       Pasture/Grass 
         "63"       Forest 
         "64"       Shrubland 
         "65"       Barren 

Raster 
Attribute Domain Values and Definitions: CROPS 66-80 

Categorization Code   Land Cover 
         "66"       Cherries 
         "67"       Peaches 
         "68"       Apples 
         "69"       Grapes 
         "70"       Christmas Trees 
         "71"       Other Tree Crops 
         "72"       Citrus 
         "74"       Pecans 
         "75"       Almonds 
         "76"       Walnuts 
         "77"       Pears 

Raster 
Attribute Domain Values and Definitions: OTHER 81-109 

Categorization Code   Land Cover 
         "81"       Clouds/No Data 
         "82"       Developed 
         "83"       Water 
         "87"       Wetlands 
         "88"       Nonag/Undefined 
         "92"       Aquaculture 

Raster 
Attribute Domain Values and Definitions: NLCD-DERIVED CLASSES 110-195 

Categorization Code   Land Cover 
        "111"       Open Water 
        "112"       Perennial Ice/Snow 
        "121"       Developed/Open Space 
        "122"       Developed/Low Intensity 
        "123"       Developed/Med Intensity 
        "124"       Developed/High Intensity 
        "131"       Barren 
        "141"       Deciduous Forest 
        "142"       Evergreen Forest 
        "143"       Mixed Forest 
        "152"       Shrubland 
        "176"       Grassland/Pasture 
        "190"       Woody Wetlands 
        "195"       Herbaceous Wetlands 

Raster 
Attribute Domain Values and Definitions: CROPS 195-255 

Categorization Code   Land Cover 
        "204"       Pistachios 
        "205"       Triticale 
        "206"       Carrots 
        "207"       Asparagus 
        "208"       Garlic 
        "209"       Cantaloupes 
        "210"       Prunes 
        "211"       Olives 
        "212"       Oranges 
        "213"       Honeydew Melons 
        "214"       Broccoli 
        "215"       Avocados 
        "216"       Peppers 
        "217"       Pomegranates 
        "218"       Nectarines 
        "219"       Greens 
        "220"       Plums 
        "221"       Strawberries 
        "222"       Squash 
        "223"       Apricots 
        "224"       Vetch 
        "225"       Dbl Crop WinWht/Corn 
        "226"       Dbl Crop Oats/Corn 
        "227"       Lettuce 
        "228"       Dbl Crop Triticale/Corn 
        "229"       Pumpkins 
        "230"       Dbl Crop Lettuce/Durum Wht 
        "231"       Dbl Crop Lettuce/Cantaloupe 
        "232"       Dbl Crop Lettuce/Cotton 
        "233"       Dbl Crop Lettuce/Barley 
        "234"       Dbl Crop Durum Wht/Sorghum 
        "235"       Dbl Crop Barley/Sorghum 
        "236"       Dbl Crop WinWht/Sorghum 
        "237"       Dbl Crop Barley/Corn 
        "238"       Dbl Crop WinWht/Cotton 
        "239"       Dbl Crop Soybeans/Cotton 
        "240"       Dbl Crop Soybeans/Oats 
        "241"       Dbl Crop Corn/Soybeans 
        "242"       Blueberries 
        "243"       Cabbage 
        "244"       Cauliflower 
        "245"       Celery 
        "246"       Radishes 
        "247"       Turnips 
        "248"       Eggplants 
        "249"       Gourds 
        "250"       Cranberries 
        "254"       Dbl Crop Barley/Soybeans 
'''