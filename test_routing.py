#!/usr/bin/env python3
"""
Test script to demonstrate pgRouting queries on Devon OSM data.
Uses routing_edges table created by setup_routing.py.
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

def main():
    print("=" * 60)
    print("pgRouting Test - Devon Walking Routes")
    print("=" * 60)
    
    database_url = get_database_url()
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    
    print("\n[1] Finding sample vertices...")
    cur.execute("""
        SELECT id, ST_X(the_geom) as lon, ST_Y(the_geom) as lat
        FROM routing_edges_vertices_pgr
        LIMIT 5;
    """)
    vertices = cur.fetchall()
    print("  Sample vertices (Web Mercator coords):")
    for v in vertices:
        print(f"    Vertex {v[0]}: ({v[1]:.2f}, {v[2]:.2f})")
    
    if len(vertices) < 2:
        print("  Not enough vertices for routing test")
        return
    
    start_vertex = vertices[0][0]
    end_vertex = vertices[-1][0]
    
    print(f"\n[2] Running Dijkstra shortest path from vertex {start_vertex} to {end_vertex}...")
    cur.execute("""
        SELECT seq, node, edge, cost, agg_cost
        FROM pgr_dijkstra(
            'SELECT edge_id AS id, source, target, cost, reverse_cost 
             FROM routing_edges 
             WHERE cost IS NOT NULL',
            %s, %s,
            directed := false
        )
        LIMIT 20;
    """, (start_vertex, end_vertex))
    
    route = cur.fetchall()
    if route:
        print(f"  Found route with {len(route)} segments (showing first 20):")
        print("  Seq | Node      | Edge        | Cost      | Total Cost")
        print("  " + "-" * 55)
        for row in route:
            print(f"  {row[0]:3} | {row[1]:9} | {row[2]:11} | {row[3]:9.2f} | {row[4]:10.2f}")
        
        total_cost = route[-1][4] if route else 0
        print(f"\n  Total route cost: {total_cost:.2f} meters (weighted)")
    else:
        print("  No route found between these vertices")
    
    print("\n[3] Finding a longer test route across Devon...")
    cur.execute("""
        SELECT 
            a.id as start_id,
            b.id as end_id,
            ST_Distance(a.the_geom, b.the_geom) as straight_line_dist
        FROM routing_edges_vertices_pgr a,
             routing_edges_vertices_pgr b
        WHERE a.id < b.id
        AND ST_Distance(a.the_geom, b.the_geom) > 5000
        AND ST_Distance(a.the_geom, b.the_geom) < 10000
        ORDER BY random()
        LIMIT 1;
    """)
    long_route = cur.fetchone()
    
    if long_route:
        start_v, end_v, distance = long_route
        print(f"  Testing route: vertex {start_v} to {end_v}")
        print(f"  Straight-line distance: {distance/1000:.2f} km")
        
        cur.execute("""
            SELECT COUNT(*), SUM(cost)
            FROM pgr_dijkstra(
                'SELECT edge_id AS id, source, target, cost, reverse_cost 
                 FROM routing_edges 
                 WHERE cost IS NOT NULL',
                %s, %s,
                directed := false
            );
        """, (start_v, end_v))
        result = cur.fetchone()
        if result and result[0]:
            print(f"  Route segments: {result[0]}")
            print(f"  Total weighted cost: {result[1]/1000:.2f} km")
    
    print("\n[4] Routing network statistics...")
    cur.execute("""
        SELECT 
            COUNT(*) as total_edges,
            SUM(CASE WHEN source IS NOT NULL AND target IS NOT NULL THEN 1 ELSE 0 END) as connected_edges,
            SUM(ST_Length(way))/1000 as total_km
        FROM routing_edges
        WHERE highway IS NOT NULL;
    """)
    stats = cur.fetchone()
    print(f"  Total highway edges: {stats[0]:,}")
    print(f"  Connected edges: {stats[1]:,}")
    print(f"  Total network length: {stats[2]:,.0f} km")
    
    cur.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("Routing test complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
