#Build docker images
cd ds_maestro
docker build -t ds_maestro .
cd ../ds_soloist
docker build -t ds_soloist .
cd ../ds_cubesearch
docker build -t ds_cubesearch .
cd ../ds_executive
docker build -t ds_executive .
