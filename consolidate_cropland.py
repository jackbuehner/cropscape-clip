import numpy
import os
import rasterio

pixel_values = {
  'background': [0],
  'crops': list(range(1, 61)) + list(range(66, 81)) + list(range(195, 256)),
  'idle': [61],
  'grassland': [62, 176],
  'forest': [63, 141, 142, 143],
  'shrubland': [64, 152],
  'barren': [65, 131],
  'nodata': [81, 88],
  'developed': [82],
  'developed_open': [121],
  'developed_low': [122],
  'developed_med': [123],
  'developed_high': [124],
  'water': [83, 111, 112],
  'wetlands': [87, 190],
  'woody_wetlands': [190],
  'aquaculture': [92],
}

colormap = {
  0: (0, 0, 0), # background, nodata
  1: (147, 105, 48), # crops
  2: (100, 100, 100), # idle
  3: (74, 59, 7), # grassland
  4: (53, 65, 22), # forest
  5: (78, 67, 27), # shrubland
  6: (50, 47, 36), # barren
  10: (195, 29, 20),  # developed
  11: (60, 32, 32), # developed_open
  12: (106, 47, 31), # developed_low
  13: (195, 29, 20), # developed_med
  14: (139, 17, 11), # developed_high
  20: (72, 93, 133), # water
  21: (50, 103, 132), # wetlands
  22: (42, 45, 47), # woody_wetlands
  28: (64, 76, 97), # aquaculture
  255: (0, 0, 0) # missing
}

def consolidate_cropland(input_raster_path: str, output_raster_path: str):
    """
    Consolidate cropland data by reclassifying cropland data layer rasters
    such that cropland is a single pixel value. Other pixel types are also
    grouped together (e.g, all forest, all develolped land, etc.).
    """
    # open the raster and lock it in the filesystem while working on it
    raster = rasterio.open(input_raster_path)
    
    # create new array where all values in the band of 2 becomes 1 and everything else becomes 0
    # in order to create an array that shows where developed land is located
    band1 = raster.read(1)
    recalc: numpy.ndarray[float] = numpy.where(
      numpy.isin(band1, pixel_values['background']), 0,
      numpy.where(
        numpy.isin(band1, pixel_values['crops']), 1,
        numpy.where(
          numpy.isin(band1, pixel_values['idle']), 2,
          numpy.where(
            numpy.isin(band1, pixel_values['grassland']), 3,
            numpy.where(
              numpy.isin(band1, pixel_values['forest']), 4,
              numpy.where(
                numpy.isin(band1, pixel_values['shrubland']), 5,
                numpy.where(
                  numpy.isin(band1, pixel_values['barren']), 6,
                  numpy.where(
                    numpy.isin(band1, pixel_values['nodata']), 0,
                    numpy.where(
                      numpy.isin(band1, pixel_values['developed']), 10,
                      numpy.where(
                        numpy.isin(band1, pixel_values['developed_open']), 11,
                        numpy.where(
                          numpy.isin(band1, pixel_values['developed_low']), 12,
                          numpy.where(
                            numpy.isin(band1, pixel_values['developed_med']), 13,
                            numpy.where(
                              numpy.isin(band1, pixel_values['developed_high']), 14,
                              numpy.where(
                                numpy.isin(band1, pixel_values['water']), 20,
                                numpy.where(
                                  numpy.isin(band1, pixel_values['wetlands']), 21,
                                  numpy.where(
                                    numpy.isin(band1, pixel_values['woody_wetlands']), 22,
                                    numpy.where(
                                      numpy.isin(band1, pixel_values['aquaculture']), 28,
                                      255
                                    )
                                  )
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
    
    # export the extracted band pixel values
    # with the extracted band as black pixels and the rest as transparent white pixels
    out_profile = raster.profile.copy()
    out_profile.update(nodata=0)
    with rasterio.open(output_raster_path, "w", **out_profile) as dest:
      dest.write(recalc, 1)
      dest.write_colormap(1, colormap)

    # remove the lock on the raster
    raster.close()
    
    return recalc
 
def consilidate_cropland_in_folder(input_folder: str, output_folder: str):
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
      
      consolidate_cropland(file_path, f'{dir_path}{output_folder}/{year}_30m_cdls.tif')
      
      print('  ☑ Done')

  print('🏁 Finished')
  
    
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