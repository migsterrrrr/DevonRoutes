#!/usr/bin/env python3
"""
Script to download and import Devon OSM data into PostgreSQL with PostGIS.
"""

import os
import subprocess
import urllib.request
import sys

OSM_URL = "https://download.geofabrik.de/europe/united-kingdom/england/devon-latest.osm.pbf"
OSM_FILE = "devon-latest.osm.pbf"

def get_database_url():
    """Get database connection string from environment."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)
    return database_url

def download_osm_data():
    """Download the OSM PBF file if not already present."""
    if os.path.exists(OSM_FILE):
        print(f"OSM file '{OSM_FILE}' already exists, skipping download.")
        return
    
    print(f"Downloading OSM data from {OSM_URL}...")
    print("This may take a few minutes depending on your connection speed.")
    
    def report_progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            percent = min(100, downloaded * 100 / total_size)
            mb_downloaded = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            sys.stdout.write(f"\rProgress: {percent:.1f}% ({mb_downloaded:.1f} MB / {mb_total:.1f} MB)")
            sys.stdout.flush()
    
    urllib.request.urlretrieve(OSM_URL, OSM_FILE, report_progress)
    print("\nDownload complete!")

def enable_postgis(database_url):
    """Enable PostGIS extension in the database."""
    print("Enabling PostGIS extension...")
    
    import psycopg2
    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
    
    cur.execute("SELECT PostGIS_Version();")
    version = cur.fetchone()[0]
    print(f"PostGIS version: {version}")
    
    cur.close()
    conn.close()
    print("PostGIS extension enabled successfully!")

def import_osm_data(database_url):
    """Import OSM data using osm2pgsql."""
    print(f"Importing OSM data from {OSM_FILE}...")
    print("This process may take several minutes for a region like Devon.")
    
    cmd = [
        "osm2pgsql",
        "--create",
        "--slim",
        "-d", database_url,
        "-S", "/nix/store/*/share/osm2pgsql/default.style" if not os.path.exists("default.style") else "default.style",
        OSM_FILE
    ]
    
    result = subprocess.run(
        ["osm2pgsql", "--create", "--slim", "-d", database_url, OSM_FILE],
        capture_output=False
    )
    
    if result.returncode != 0:
        print("ERROR: osm2pgsql import failed")
        sys.exit(1)
    
    print("OSM data import complete!")

def verify_import(database_url):
    """Verify that data was imported successfully."""
    print("\nVerifying import...")
    
    import psycopg2
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    
    tables = ["planet_osm_point", "planet_osm_line", "planet_osm_polygon", "planet_osm_roads"]
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")
    
    cur.close()
    conn.close()
    print("\nImport verification complete!")

def main():
    print("=" * 60)
    print("Devon OSM Data Import Script")
    print("=" * 60)
    
    database_url = get_database_url()
    print(f"Database connection configured.")
    
    download_osm_data()
    enable_postgis(database_url)
    import_osm_data(database_url)
    verify_import(database_url)
    
    print("\n" + "=" * 60)
    print("Import completed successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
