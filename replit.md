# Devon OSM Data Import Project

## Overview
This project downloads and imports OpenStreetMap (OSM) data for Devon, UK into a PostgreSQL database with PostGIS support.

## Project Structure
- `import_osm.py` - Main script to download and import OSM data

## Data Source
- URL: https://download.geofabrik.de/europe/united-kingdom/england/devon-latest.osm.pbf
- Region: Devon, England, UK

## Database Tables
After import, the following tables will be available:
- `planet_osm_point` - Point features (POIs, etc.)
- `planet_osm_line` - Linear features (roads, rivers, etc.)
- `planet_osm_polygon` - Polygon features (buildings, areas, etc.)
- `planet_osm_roads` - Road network subset

## Usage
Run the import script:
```bash
python import_osm.py
```

## Dependencies
- Python 3.11
- psycopg2-binary
- osm2pgsql (system)
- PostGIS extension

## Recent Changes
- 2025-12-31: Initial project setup with OSM import script
