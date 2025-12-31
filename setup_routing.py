#!/usr/bin/env python3
"""
Script to set up pgRouting topology on OSM data.
Creates a routing_edges table from planet_osm_line for comprehensive walking/hiking coverage.
"""

import os
import sys
import psycopg2

def get_database_url():
    """Get database connection string from environment."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)
    return database_url

def execute_sql(conn, sql, description):
    """Execute SQL and print status."""
    print(f"  {description}...")
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        print(f"  ✓ {description} complete")
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f"  ✗ Error: {e}")
        return False
    finally:
        cur.close()

def query_sql(conn, sql):
    """Execute SQL query and return results."""
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    return result

def main():
    print("=" * 60)
    print("pgRouting Topology Setup for Devon OSM Data")
    print("=" * 60)
    
    database_url = get_database_url()
    conn = psycopg2.connect(database_url)
    
    print("\n[Step 1/6] Enabling pgRouting extension...")
    execute_sql(conn, "CREATE EXTENSION IF NOT EXISTS pgrouting;", "Enable pgrouting")
    
    version = query_sql(conn, "SELECT pgr_version();")
    print(f"  pgRouting version: {version[0][0]}")
    
    print("\n[Step 2/6] Creating routing_edges table from planet_osm_line...")
    print("  (This avoids modifying the original OSM data)")
    
    execute_sql(conn, "DROP TABLE IF EXISTS routing_edges_vertices_pgr CASCADE;", "Drop old vertices table")
    execute_sql(conn, "DROP TABLE IF EXISTS routing_edges CASCADE;", "Drop old routing_edges table")
    
    execute_sql(conn, """
        CREATE TABLE routing_edges AS
        SELECT 
            row_number() OVER () as edge_id,
            osm_id,
            highway,
            name,
            way,
            NULL::integer as source,
            NULL::integer as target,
            NULL::double precision as cost,
            NULL::double precision as reverse_cost
        FROM planet_osm_line
        WHERE way IS NOT NULL;
    """, "Create routing_edges table with unique edge_id")
    
    execute_sql(conn, """
        ALTER TABLE routing_edges ADD PRIMARY KEY (edge_id);
    """, "Add primary key on edge_id")
    
    edge_count = query_sql(conn, "SELECT COUNT(*) FROM routing_edges;")
    print(f"  Created {edge_count[0][0]:,} edges")
    
    print("\n[Step 3/6] Creating spatial index...")
    execute_sql(conn, """
        CREATE INDEX IF NOT EXISTS routing_edges_way_idx 
        ON routing_edges USING GIST (way);
    """, "Create spatial index")
    
    print("\n[Step 4/6] Creating routing topology...")
    print("  This may take several minutes for a large dataset like Devon...")
    print("  Using tolerance of 1.0 meters (SRID 3857)")
    
    cur = conn.cursor()
    cur.execute("""
        SELECT pgr_createTopology(
            'routing_edges',
            1.0,
            'way',
            'edge_id'
        );
    """)
    result = cur.fetchone()
    conn.commit()
    cur.close()
    print(f"  ✓ Topology creation result: {result[0]}")
    
    vertices = query_sql(conn, "SELECT COUNT(*) FROM routing_edges_vertices_pgr;")
    print(f"  Created {vertices[0][0]:,} vertices")
    
    print("\n[Step 5/6] Creating indexes on source/target...")
    execute_sql(conn, """
        CREATE INDEX IF NOT EXISTS routing_edges_source_idx ON routing_edges(source);
    """, "Create source index")
    execute_sql(conn, """
        CREATE INDEX IF NOT EXISTS routing_edges_target_idx ON routing_edges(target);
    """, "Create target index")
    
    print("\n[Step 6/6] Calculating costs based on road/path type...")
    execute_sql(conn, """
        UPDATE routing_edges
        SET cost = ST_Length(way) * 
            CASE 
                WHEN highway IN ('footway', 'path', 'pedestrian', 'steps') THEN 1.0
                WHEN highway IN ('track', 'bridleway') THEN 1.1
                WHEN highway IN ('cycleway') THEN 1.2
                WHEN highway IN ('residential', 'living_street', 'service') THEN 1.3
                WHEN highway IN ('unclassified', 'tertiary', 'tertiary_link') THEN 1.4
                WHEN highway IN ('secondary', 'secondary_link') THEN 1.5
                WHEN highway IN ('primary', 'primary_link') THEN 1.8
                WHEN highway IN ('trunk', 'trunk_link') THEN 2.5
                WHEN highway IN ('motorway', 'motorway_link') THEN 10.0
                ELSE 1.5
            END
        WHERE highway IS NOT NULL AND source IS NOT NULL;
    """, "Calculate walking/hiking costs")
    
    execute_sql(conn, """
        UPDATE routing_edges
        SET reverse_cost = cost
        WHERE cost IS NOT NULL;
    """, "Set reverse costs (bidirectional)")
    
    print("\n" + "=" * 60)
    print("Routing Setup Complete!")
    print("=" * 60)
    
    print("\nSummary:")
    stats = query_sql(conn, """
        SELECT 
            COUNT(*) as total_edges,
            COUNT(CASE WHEN source IS NOT NULL THEN 1 END) as routable_edges,
            COUNT(CASE WHEN cost IS NOT NULL THEN 1 END) as costed_edges
        FROM routing_edges;
    """)
    print(f"  Total edges: {stats[0][0]:,}")
    print(f"  Routable edges (in network): {stats[0][1]:,}")
    print(f"  Costed edges: {stats[0][2]:,}")
    
    highway_stats = query_sql(conn, """
        SELECT highway, COUNT(*) as count
        FROM routing_edges
        WHERE highway IS NOT NULL AND source IS NOT NULL
        GROUP BY highway
        ORDER BY count DESC
        LIMIT 10;
    """)
    print("\n  Top 10 highway types in routing network:")
    for row in highway_stats:
        print(f"    {row[0]}: {row[1]:,}")
    
    conn.close()
    print("\nRouting is ready! Run test_routing.py to try a sample route.")

if __name__ == "__main__":
    main()
