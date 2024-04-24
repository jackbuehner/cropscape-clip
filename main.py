import json
import os

import rich

from clip_cropscape_to_area_of_interest import \
    clip_cropscape_to_area_of_interest
from consolidate_cropland import consilidate_cropland_in_folder
from summarize_raster import summarize_raster

console = rich.console.Console()
status = console.status('[bold green]Working...[/bold green]')
status.start()

# print(os.getcwd())

# limit to our area of interest by clipping first, which will also make subsequent steps faster
status.update('Clipping cropscape data to area of interest...')
clip_cropscape_to_area_of_interest('./input', './input/area_of_interest.shp', './working/clipped')
console.log('Cropscape data clipped to area of interest')

# consolidate similar pixel classes, including all cropland classes
status.update('Consolidating cropland classes...')
consilidate_cropland_in_folder('/working/clipped', '/output/consolidated')
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
    summary_data.append({
      'cropland_year': int(year),
      'zcta_year': 2010,
      'data': summarize_raster(
        f'{file_root}.tif',
        f'{file_root}.json',
        './input/US_zcta_2010.shp',
        './working/zcta',
        status=status,
        status_prefix=f'[{year}|2010] '
      ) 
    })
with open('./output/summary_data.json', "w") as file:
  json.dump(summary_data, file, indent=2) 
  console.log('Summary data saved to ./output/summary_data.json')
