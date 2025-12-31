#!/usr/bin/env python3
"""
Setup pgRouting topology for walking/hiking network in Devon.
Creates a filtered walking_network table from OSM data.
"""

import os
import psycopg2
from psycopg2 import sql

def get_db_connection():
    """Get database connection from DATABASE_URL."""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(database_url)

def run_sql(cur, description, query, params=None):
    """Execute SQL with status reporting."""
    print(f"  {description}...")
    cur.execute(query, params)
    print(f"  ✓ {description} complete")

def main():
    print("=" * 60)
    print("pgRouting Walking Network Setup for Devon OSM Data")
    print("=" * 60)
    
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        print("\n[Step 1/5] Enabling pgRouting extension...")
        run_sql(cur, "Enable pgrouting", 
                "CREATE EXTENSION IF NOT EXISTS pgrouting;")
        
        cur.execute("SELECT pgr_version();")
        version = cur.fetchone()[0]
        print(f"  pgRouting version: {version}")
        
        print("\n[Step 2/5] Creating walking_network table...")
        print("  (Filtering for pedestrian-accessible ways)")
        
        run_sql(cur, "Drop old walking_network table",
                "DROP TABLE IF EXISTS walking_network CASCADE;")
        
        create_walking_network_sql = """
            CREATE TABLE walking_network AS
            SELECT 
                way,
                osm_id,
                name,
                highway,
                surface,
                access,
                NULL::BIGINT AS source,
                NULL::BIGINT AS target
            FROM planet_osm_line
            WHERE (
                highway IN ('footway', 'path', 'bridleway', 'steps', 'pedestrian', 
                           'living_street', 'residential', 'track', 'cycleway', 'service')
                OR foot IN ('yes', 'designated')
            )
            AND (access IS NULL OR access != 'private')
            AND (foot IS NULL OR foot != 'no');
        """
        
        run_sql(cur, "Create walking_network table with filtered ways",
                create_walking_network_sql)
        
        print("\n[Step 3/5] Adding edge_id primary key...")
        run_sql(cur, "Add edge_id column",
                "ALTER TABLE walking_network ADD COLUMN edge_id SERIAL PRIMARY KEY;")
        
        print("\n[Step 4/5] Adding length and cost columns...")
        run_sql(cur, "Add length_m column",
                "ALTER TABLE walking_network ADD COLUMN length_m FLOAT8;")
        
        run_sql(cur, "Calculate length_m using geography for accurate meters",
                "UPDATE walking_network SET length_m = ST_Length(way::geography);")
        
        run_sql(cur, "Add cost column",
                "ALTER TABLE walking_network ADD COLUMN cost FLOAT8;")
        
        run_sql(cur, "Add reverse_cost column",
                "ALTER TABLE walking_network ADD COLUMN reverse_cost FLOAT8;")
        
        run_sql(cur, "Set cost = length_m",
                "UPDATE walking_network SET cost = length_m;")
        
        run_sql(cur, "Set reverse_cost = length_m",
                "UPDATE walking_network SET reverse_cost = length_m;")
        
        print("\n[Step 5/5] Creating spatial index...")
        run_sql(cur, "Create spatial index on way column",
                "CREATE INDEX IF NOT EXISTS walking_network_way_idx ON walking_network USING GIST(way);")
        
        cur.execute("SELECT COUNT(*) FROM walking_network;")
        count = cur.fetchone()[0]
        
        cur.execute("SELECT SUM(length_m) / 1000.0 FROM walking_network;")
        total_km = cur.fetchone()[0]
        
        print("\n" + "=" * 60)
        print("Walking Network Setup Complete!")
        print("=" * 60)
        print(f"  Total walkable segments: {count:,}")
        print(f"  Total network length: {total_km:,.1f} km")
        print("\n  Table columns: edge_id, way, osm_id, name, highway,")
        print("                 surface, access, source, target,")
        print("                 length_m, cost, reverse_cost")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
