INSERT INTO road_segments (odm_id, osm_type, name, ref, road_type, speed_limit_kmh, tags, geom)
SELECT
    osm_id AS osm_id,
    highway AS osm_type,
    name,
    ref,
    -- Initial classification for 'type' based on common German road types
    CASE
        WHEN highway = 'motorway' THEN 'Autobahn'
        WHEN highway = 'trunk' THEN 'Bundesstrasse'
        WHEN highway = 'primary' THEN 'Landesstrasse' -- Often L-roads or major city roads
        WHEN highway = 'secondary' THEN 'Landesstrasse' -- Also can be L-roads or significant local roads
        WHEN highway = 'tertiary' THEN 'Stadtstrasse'  -- Inner city roads
        WHEN highway = 'residential' THEN 'Residential'
        WHEN highway = 'unclassified' THEN 'Unclassified' -- Roads not clearly classified
        WHEN highway = 'service' THEN 'Service Road'
        ELSE 'Other Road' -- Catch-all for less relevant types, you might want to filter these out
    END AS road_type,
    CASE
        WHEN tags -> 'maxspeed' ~ '^[0-9]+$' THEN CAST(tags -> 'maxspeed' AS INTEGER)
        WHEN tags -> 'maxspeed' IN ('walk', 'inf') THEN NULL -- Not applicable or unlimited
        WHEN tags -> 'maxspeed' = 'DE:urban' THEN 50 -- Default urban speed limit in Germany
        WHEN tags -> 'maxspeed' = 'DE:rural' THEN 100 -- Default rural speed limit in Germany
        ELSE NULL -- For other unparseable maxspeed values
    END AS speed_limit_kmh,
    tags,
    ST_Transform(way, 4326) AS geom -- 'way' is the geometry column from planet_osm_line
FROM
    planet_osm_line
WHERE
    highway IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'unclassified', 'service')
    AND NOT (tunnel = 'yes' AND highway = 'footway') -- Exclude footways in tunnels
    AND NOT (bridge = 'yes' AND highway = 'footway'); -- Exclude footways on bridges

-- Update all road segments best guessing the speed limit, based on German road types.
UPDATE road_segments
SET speed_limit_kmh = CASE
    WHEN type = 'Autobahn' AND speed_limit_kmh IS NULL THEN 130 -- German Autobahn advisory speed (no general limit)
    WHEN type IN ('Bundesstrasse', 'Landesstrasse', 'Unclassified') AND speed_limit_kmh IS NULL THEN 100 -- Rural roads
    WHEN type IN ('Stadtstrasse', 'Residential', 'Service Road') AND speed_limit_kmh IS NULL THEN 50 -- Urban areas
    ELSE speed_limit_kmh -- Keep existing specific speed limits
END
WHERE speed_limit_kmh IS NULL; -- Only update rows where speed_limit_kmh is currently NULL

-- Process Traffic Lights
INSERT INTO traffic_lights (osm_id, name, is_pedestrian_crossing_light, tags, geom)
SELECT
    osm_id AS id,
    name,
    CASE
        WHEN tags -> 'crossing' = 'traffic_signals' THEN TRUE -- Use tags->'crossing' for hstore
        ELSE FALSE
    END AS is_pedestrian_crossing_light,
    tags,
    ST_Transform(way, 4326) AS geom -- 'way' is the geometry column for points in planet_osm_point
FROM
    planet_osm_point
WHERE
    -- Select main traffic signals for vehicles (check highway tag directly)
    highway = 'traffic_signals'
    OR
    -- Select traffic signals specifically for crossings (check tags hstore for crossing=traffic_signals)
    (tags ? 'crossing' AND tags -> 'crossing' = 'traffic_signals');

-- Optional: Remove duplicates if any OSM object was tagged both ways
-- This might not be strictly necessary if OSM mappers are consistent,
-- but good for robustness.
DELETE FROM traffic_lights
WHERE ctid IN (
    SELECT ctid FROM (
        SELECT
            ctid,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) as rn
        FROM traffic_lights
    ) t WHERE t.rn > 1
);
