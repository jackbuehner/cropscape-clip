This script clips cropscape data from the USDA to the provided clip shapefile.

1. Download data for all years of interest from the [https://www.nass.usda.gov/Research_and_Science/Cropland/Release/index.php](https://www.nass.usda.gov/Research_and_Science/Cropland/Release/index.php) to the folder with this README.
2. Extract to the individual folders for each year in the "input" directory (e.g., `input/2008_30m_cdls`).
3. Add a clip shape with the name `input/area_of_interest.shp`.
4. Create a python virtual environment via your terminal: `python venv .venv`.
5. Activate the python virtual environment via your terminal: `source .venv/Scripts/activate`.
6. Install dependencies via your terminal `pip install -r requirements.txt`.
7. Run `clip_cropscape_to_area_of_interest.py`.
