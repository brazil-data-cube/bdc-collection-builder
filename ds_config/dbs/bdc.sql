CREATE DATABASE IF NOT EXISTS bdc


CREATE TABLE IF NOT EXISTS grs_schemas
(
    id VARCHAR(20) UNIQUE NOT NULL,
    description VARCHAR(64) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS tiles
(
    id VARCHAR(20) UNIQUE NOT NULL,
    grs_schema VARCHAR(20) NOT NULL REFERENCES grs_schemas(id),
    geom_wgs84 GEOMETRY,
    geom GEOMETRY,
    PRIMARY KEY(grs_schema, id)
);

CREATE TABLE IF NOT EXISTS providers
(
    id VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(64) UNIQUE NOT NULL,
    storage_type VARCHAR(16) NOT NULL,
    description VARCHAR(64) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS raster_chunk_schemas
(
    id VARCHAR(20) UNIQUE NOT NULL,
    raster_size_x INTEGER,
    raster_size_y INTEGER,
    raster_size_t INTEGER,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS spatial_resolution_schemas
(
    id VARCHAR(20) UNIQUE NOT NULL,
    resolution_x DOUBLE PRECISION NOT NULL,
    resolution_y DOUBLE PRECISION NOT NULL,
    resolution_unit VARCHAR(16) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS temporal_composition_schemas
(
    id VARCHAR(20) UNIQUE NOT NULL,
    temporal_composite_unit VARCHAR(16) NOT NULL,
    temporal_schema VARCHAR(16) NOT NULL,
    temporal_composite_t VARCHAR(16) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS cube_collections
(
    id VARCHAR(20) NOT NULL,
    spatial_resolution_schema VARCHAR(20) NOT NULL REFERENCES spatial_resolution_schemas(id),
    temporal_composition_schema VARCHAR(20) NOT NULL REFERENCES temporal_composition_schemas(id),
    raster_chunk_schema VARCHAR(20) NOT NULL REFERENCES raster_chunk_schemas(id),
    grs_schema VARCHAR(20) NOT NULL REFERENCES grs_schemas(id),
    version VARCHAR(16) NOT NULL,
    description VARCHAR(64) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS composite_functions
(
    id VARCHAR(20) UNIQUE NOT NULL,
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    description VARCHAR(64),
    PRIMARY KEY(id, cube_collection)
);

CREATE TABLE IF NOT EXISTS bands
(
    id VARCHAR(20) UNIQUE NOT NULL,
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    min REAL,
    max REAL,
    fill INTEGER,
    scale VARCHAR(16),
    commom_name VARCHAR(16),
    data_type VARCHAR(16),
    mime_type VARCHAR(16),
    description VARCHAR(64),
    PRIMARY KEY(id, cube_collection)
);

CREATE TABLE IF NOT EXISTS cube_items
(
    id SERIAL UNIQUE NOT NULL,
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    grs_schema VARCHAR(20) NOT NULL REFERENCES grs_schemas(id),
    tile VARCHAR(20) NOT NULL REFERENCES tiles(id),
    composite_function VARCHAR(20) REFERENCES composite_functions(id),
    item_date DATE NOT NULL,
    composite_start DATE NOT NULL,
    composite_end DATE,
    quicklook TEXT,
    PRIMARY KEY(id, cube_collection, grs_schema, tile, item_date, composite_function)
);

CREATE TABLE IF NOT EXISTS assets
(
    id SERIAL UNIQUE NOT NULL,
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    band VARCHAR(20) REFERENCES bands(id),
    grs_schema VARCHAR(20) REFERENCES grs_schemas(id),
    tile VARCHAR(20) REFERENCES tiles(id),
    cube_item INTEGER REFERENCES cube_items(id),
    PRIMARY KEY(cube_collection, id)
);

CREATE TABLE IF NOT EXISTS asset_providers
(
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    provider VARCHAR(20) NOT NULL REFERENCES providers(id),
    storage_info VARCHAR(32),
    description VARCHAR(64),
    PRIMARY KEY(cube_collection, provider)
);

CREATE TABLE IF NOT EXISTS asset_links
(
    cube_collection VARCHAR(20) NOT NULL,
    provider VARCHAR(20) NOT NULL,
    asset INTEGER NOT NULL REFERENCES assets(id),
    file_path VARCHAR(64),
    PRIMARY KEY(cube_collection, provider, asset)
);

CREATE TABLE IF NOT EXISTS cubes
(
    id VARCHAR(20) UNIQUE NOT NULL,
    cube_collection VARCHAR(20) REFERENCES cube_collections(id),
    provider VARCHAR(20) REFERENCES providers(id),
    composite_function VARCHAR(20) REFERENCES composite_functions(id),
    oauth_info VARCHAR(16),
    description VARCHAR(64),
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS cube_tiles
(
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    grs_schema VARCHAR(20) NOT NULL REFERENCES grs_schemas(id),
    tile VARCHAR(20) NOT NULL REFERENCES tiles(id),
    PRIMARY KEY(cube_collection, grs_schema, tile)
);

CREATE TABLE IF NOT EXISTS band_compositions
(
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    band VARCHAR(20) NOT NULL REFERENCES bands(id),
    product VARCHAR(16) UNIQUE NOT NULL,
    product_band VARCHAR(16) UNIQUE NOT NULL,
    description VARCHAR(64),
    PRIMARY KEY(cube_collection, band, product, product_band)
);

CREATE TABLE IF NOT EXISTS asset_compositions
(
    cube_collection VARCHAR(20) NOT NULL REFERENCES cube_collections(id),
    asset INTEGER NOT NULL REFERENCES assets(id),
    product VARCHAR(64) NOT NULL REFERENCES band_compositions(product),
    product_band VARCHAR(16) NOT NULL REFERENCES band_compositions(product_band),
    reference_date DATE NOT NULL,
    file_path TEXT,
    PRIMARY KEY(cube_collection, asset, product, product_band, reference_date)
);
