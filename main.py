from clip_cropscape_to_area_of_interest import clip_cropscape_to_area_of_interest
from consolidate_cropland import consilidate_all_cropland

# limit to our area of interest by clipping first, which will also make subsequent steps faster
clip_cropscape_to_area_of_interest('/input', '/input/area_of_interest.shp', '/working/clipped')

# consolidate similar pixel classes, inlcuing all cropland classes
consilidate_all_cropland('/working/clipped', '/output')