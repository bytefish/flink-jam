DROP TABLE IF EXISTS road_segments;

CREATE TABLE road_segments (
    osm_id BIGINT PRIMARY KEY, -- OSM IDs can be large, use BIGINT
    osm_type text,  -- e.g., 'way' (from osm2pgsql)
    road_type text,      -- e.g., 'Autobahn', 'Bundesstrasse', 'Stadtstrasse' (your classification)
    name text,       -- Road name (optional, but useful for debugging)
    ref text, -- Name of the Highway
    speed_limit_kmh INTEGER, -- Speed limit in km/h
    tags hstore, -- Tags
    geom GEOMETRY(LineString, 4326) -- The actual road geometry
);

CREATE INDEX idx_road_segments_id ON road_segments (id);
CREATE INDEX idx_road_segments_geom ON road_segments USING GIST(geom);
CREATE INDEX idx_road_segments_geom_geography ON road_segments USING gist((geom::geography));

DROP TABLE IF EXISTS traffic_lights;

-- Create the traffic_lights table
CREATE TABLE traffic_lights (
    osm_id BIGINT PRIMARY KEY, -- OSM ID of the traffic light node
    name VARCHAR(255),     -- Optional: Name of the intersection or road
    is_pedestrian_crossing_light BOOLEAN DEFAULT FALSE, -- To distinguish car signals from pedestrian signals
    tags,
    geom GEOMETRY(Point, 4326) -- The geographical point of the traffic light
);

CREATE INDEX idx_traffic_lights_id ON traffic_lights (id);
CREATE INDEX idx_traffic_lights_geom ON traffic_lights USING GIST(geom);
CREATE INDEX idx_traffic_lights_geom_geography ON traffic_lights USING gist((geom::geography));
