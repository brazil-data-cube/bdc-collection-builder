## Hands on bdc-scripts
### Before Start
User accounts are required to download images from providers. 

In order to search and obtain images from SciHub (Copernicus) (e.g. Sentinel-2 images), users must have a registered account at: https://scihub.copernicus.eu/dhus/#/self-registration and confirm validation through email. This account may take a few days to be operational when using it in scripts.


In order to search and obtain images from Earth Explorer (USGS) (e. g. Landsat-8 images), there must be a registered account at: https://ers.cr.usgs.gov/register/ and confirm validation through email.


In order to perform LaSRC atmospheric correction on Landsat-8 images, several auxiliary files are required. LADS are auxiliare files produced using MODIS data. in order to obtain LADS users must have an account at Earth Data: https://urs.earthdata.nasa.gov/home, in order to obtain a token (after the login) to use in the scripts.


In order to upload the files to AWS S3, AWS accessKeys are also required.

### Setting up the environment

git clone https://github.com/brazil-data-cube/bdc-scripts.git

insert your SciHub user and password in rc_maestro/secrets_s2.csv
insert your SciHub user and password in rc_maestro/secrets_S2.JSON
insert your USGS user and password in rc_maestro/secrets_USGS.csv
insert your AWS user and password in rc_maestro/accessKeys.csv

modify bdc_compose.yml (set ports, mount volumes, set passwords)

Create tables in db (sql files)

docker-compose -f bdc_compose.yml up

perform update LADS (if LaSRC is going to be used)

## BDC-Scripts Architecture

#### Radcor (image acquisition and publishing)
Download -> Atm correction -> publish -> upload

#### DataStorm (DS, cube builder)
warp -> merge -> blend 

### Radcor
##### Searching Sentinel-2 A/B images
curl datacube-001:5030/radcor?w=-45.90\&s=-12.74\&n=-12.6\&e=-45.80\&satsen=S2\&start=2018-09-01\&end=2018-09-31

##### Searching Landsat-8/OLI images
curl datacube-001:5030/radcor?w=-45.90\&s=-12.74\&n=-12.6\&e=-45.80\&satsen=LC8\&start=2018-09-01\&end=2018-09-31

insert \&action=process to start downloading

### DataStorm (DS)
#### Create Cube
curl localhost:5021/create\?datacube=L30m\&satsen=LC8\&start=2018-09-01\&end=2019-08-31\&wrs=aea_250k\&resx=30\&resy=30\&tschema=M\&bands=coastal,blue,green,red,nir,swir1,swir2,ndvi,evi,quality\&quicklook=swir2,nir,red

curl localhost:5021/create\?datacube=S1016d\&satsen=S2SR\&start=2018-09-01\&end=2019-08-31\&wrs=aea_250k\&resx=10\&resy=10\&tschema=A\&step=16\&bands=coastal,blue,green,red,redge1,redge2,redge3,nir,bnir,swir1,swir2,ndvi,evi,quality\&quicklook=swir2,nir,red

#### process cube
curl localhost:5020/process\?datacube=L30m\&pr=089098\&start=start=2018-09-01\&end=2019-08-31

#### delete cube_item reprocess cube
curl localhost:5020/deldice?datacube=L30m\&tileid=089098\&startdate=2019-04-01