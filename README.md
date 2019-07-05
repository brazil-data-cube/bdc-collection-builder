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


## Running with docker
```
run build.sh
```
### TODO : Write instructions to deploy and run aplication
