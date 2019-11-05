# BDC-Scripts - Brazil Data Cube
Infrastructure for creating brazil data cube environment in a local server machine

## Structure

- [`ds_config`](./ds_config): Definitions and system configs (Reference Grids, Spetral bands mapping, etc) 

- [`ds_cubesearch`](./ds_cubesearch): Web interface to view datacubes, download products, etc. 

- [`ds_executive`](./ds_executive): Web interface for management of datacubes (definition, creating, execution) 

- [`ds_include`](./ds_include): Include files needed by the system 

- [`ds_maestro`](./ds_maestro): Docker and components that manage the cube generation system (manage queues, tasks) 

- [`ds_opensearch`](./ds_opensearch): API opensearch to the original files repository 

- [`ds_soloist`](./ds_soloist): Docker and components that execute datacube tasks 

- [`ds_source`](./ds_source): Python source codes 

- [`ds_utils`](./ds_utils): Python API for accessing data cube and general tools


### Requirements

Make sure you have the following libraries and tools installed: 

- [`Python 3`](https://www.python.org/) 
- [Docker](https://www.docker.com/) Docker version 18.06.1 or later 
- [Docker Compose](https://docs.docker.com/compose/) docker-compose version 1.22.0 or later

## Installation 

git clone https://github.com/brazil-data-cube/bdc-scripts.git

### Before Start
User accounts are required to download images from providers. 

In order to search and obtain images from SciHub (Copernicus) (e.g. Sentinel-2 images), users must have a registered account at: https://scihub.copernicus.eu/dhus/#/self-registration and confirm validation through email. This account may take a few days to be operational when using it in scripts.


In order to search and obtain images from Earth Explorer (USGS) (e. g. Landsat-8 images), there must be a registered account at: https://ers.cr.usgs.gov/register/ and confirm validation through email.


In order to perform LaSRC atmospheric correction on Landsat-8 images, several auxiliary files are required. LADS are auxiliare files produced using MODIS data. in order to obtain LADS users must have an account at Earth Data: https://urs.earthdata.nasa.gov/home, in order to obtain a token (after the login) to use in the scripts.


In order to upload the files to AWS S3, AWS accessKeys are also required.

### Setting up passwords

insert your SciHub user and password in rc_maestro/secrets_s2.csv
insert your SciHub user and password in rc_maestro/secrets_S2.JSON
insert your USGS user and password in rc_maestro/secrets_USGS.csv
insert your AWS user and password in rc_maestro/accessKeys.csv

### Setting up environment

modify bdc_compose.yml (set ports, passwords and mount volumes)

** If landsat atmospheric correction (LaSRC) is going to be performed, user must run updateLADS.py to obtain auxiliary data for each day of the year.

### Run Docker-compose

docker-compose -f bdc_compose.yml up

** stop docker-compes: docker-compose -f bdc_compose.yml down

### Create tables in db

open PHPmy admin from browser setting its path/port and import tables to your db (sql files)


## BDC-Scripts Architecture

### Radcor (image acquisition and publishing)
[`Download`] -> [`Atm correction`] -> [`publish`] -> [`upload`]

### DataStorm (DS, cube builder)
[`warp`] -> [`merge`] -> [`blend`]

## Radcor
### Searching Sentinel-2 A/B images
curl <localhost>:<port>/radcor?w=-45.90\&s=-12.74\&n=-12.6\&e=-45.80\&satsen=S2\&start=2018-09-01\&end=2018-09-31

### Searching Landsat-8/OLI images
Consult available images:
curl <localhost-001>:<port>/radcor?w=-45.90\&s=-12.74\&n=-12.6\&e=-45.80\&satsen=LC8\&start=2018-09-01\&end=2018-09-31

Download available images:
curl <localhost-001>:<port>/radcor?w=-45.90\&s=-12.74\&n=-12.6\&e=-45.80\&satsen=LC8\&start=2018-09-01\&end=2018-09-31\&action=process

## DataStorm (DS)
### Create Cube
curl <localhost>:<port>/create\?datacube=L30m\&satsen=LC8\&start=2018-09-01\&end=2019-08-31\&wrs=aea_250k\&resx=30\&resy=30\&tschema=M\&bands=coastal,blue,green,red,nir,swir1,swir2,ndvi,evi,quality\&quicklook=swir2,nir,red

curl <localhost>:<port>/create\?datacube=S1016d\&satsen=S2SR\&start=2018-09-01\&end=2019-08-31\&wrs=aea_250k\&resx=10\&resy=10\&tschema=A\&step=16\&bands=coastal,blue,green,red,redge1,redge2,redge3,nir,bnir,swir1,swir2,ndvi,evi,quality\&quicklook=swir2,nir,red

### process cube
curl <localhost>:<port>/process\?datacube=L30m\&pr=089098\&start=start=2018-09-01\&end=2019-08-31

### delete cube_item reprocess cube
curl <localhost>:<port>/deldice?datacube=L30m\&tileid=089098\&startdate=2019-04-01
