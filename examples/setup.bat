REM This script is used to set up the environment for the Docker container and run the main.py script inside the container.
REM The script performs the following steps:
REM 1. Perform initial robocopy commands to copy input data to the local machine.
REM 2. Loop through each geodatabase and perform robocopy to copy the geodatabase to the local machine.
REM 3. Import the Docker image.
REM 4. Remove unused Docker containers.
REM 5. Loop through each geodatabase and perform the following steps:
REM    a. Start the Docker container.
REM    b. Run the main.py script inside the Docker container.
REM    c. Stop the Docker container.
REM 6. Display a message indicating that the script has completed.
REM
REM Note: This script uses the ENABLEDELAYEDEXPANSION option to enable delayed expansion of variables inside a loop.
REM This is necessary to access the loop variable inside the loop.
REM
REM Usage: setup.bat
REM Author: Jack Buehner (jackb@furman.edu) (Applied Research Team, The Shi Institute for Sustainable Communities, Furmaun University)
REM
REM This is script contains references to paths that are used by the Applied Research Team at Furman University.
REM You may need to modify the paths to match your own environment.
REM
REM The Cropland Data Layer (CDL) can be downloaded from the following website:
REM https://www.nass.usda.gov/Research_and_Science/Cropland/Release/index.php
REM
REM The Urban Areas 2020 dataset can be downloaded from the United States Census from their TIGERweb REST service:
REM https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer/88

@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

REM set wslconfig
echo Setting WSL configuration...
echo [wsl2]^

networkingMode=mirrored^

memory=48GB^

^

[experimental]^

autoMemoryReclaim=gradual^

sparseVhd=true^

hostAddressLoopback=true> %userprofile%\.wslconfig


REM Set geodatabases
SET geodatabases=sc_pickens.gdb,

REM Perform initial robocopy commands
robocopy \\bl2022\gisprojects\CELProjects\SAgE\cropscape_multimachine_io\input\cdls\ .\input\cdls\ /E
robocopy \\bl2022\gisprojects\CELProjects\SAgE\cropscape_multimachine_io\input\south_carolina\ .\input\south_carolina\ /E
robocopy \\bl2022\gisprojects\CELProjects\SAgE\cropscape_multimachine_io\input\urban_areas_2020_corrected\ .\input\urban_areas_2020_corrected\ /E

REM Loop through each geodatabase and perform robocopy
FOR %%g in (%geodatabases%) DO (
    REM Perform the robocopy directly using the loop variable
    robocopy "\\bl2022\gisprojects\CELProjects\SAgE\cropscape_multimachine_io\input\parcels\%%g" ".\input\parcels\%%g" /E
)

REM Import the Docker image
echo Importing Docker image...
docker load -i cropscape-clip.tar

REM Remove unused Docker containers
echo ""
echo Removing unused Docker containers...
docker container prune -f

REM Start the Docker container and run commands inside it
FOR %%g in (%geodatabases%) DO (
    set "gdbname=%%~ng"

    REM Display the geodatabase name
    echo ""
    echo Processing geodatabase: !gdbname!.gdb

    REM Start the Docker container
    echo ""
    echo Starting Docker container with name cropscape_clip_!gdbname!...
    docker run -it -d --rm^
        --name cropscape_clip_!gdbname!^
        --mount type=bind,source=%cd%/logging,target=/home/app/logging^
        --mount type=bind,source=%cd%/input,target=/home/app/input^
        --mount type=bind,source="%OneDrive%/cropscape_multimachine_output/!gdbname!",target=/home/app/output^
        cropscape-clip

    REM Run the Docker container
    echo ""
    echo Running Docker container with name cropscape_clip_!gdbname!...
    docker exec -it cropscape_clip_!gdbname! python main.py^
        --gdb_path=input/parcels/!gdbname!.gdb^
        --layer_name=!gdbname!^
        --id_key=parcelnumb_no_formatting^
        --output_gpkg=output/!gdbname!.gpkg^
        --cdls_folder_path=input/cdls^
        --cdls_aoi_shp_path=input/south_carolina/area_of_interest.shp^
        --summary_output_folder_path=output

    REM Stop the Docker container
    echo ""
    echo Stopping Docker container with name cropscape_clip_!gdbname!...
    docker stop cropscape_clip_!gdbname!
)

echo ""
echo Done.
ENDLOCAL

pause