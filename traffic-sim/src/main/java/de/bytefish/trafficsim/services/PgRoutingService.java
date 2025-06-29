package de.bytefish.trafficsim.services;

import de.bytefish.trafficsim.models.Point;
import de.bytefish.trafficsim.models.SimulatedRouteSegment;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PgRoutingService {

    private final DataSource dataSource;

    public PgRoutingService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public OptionalLong getVertexId(Point point) throws Exception {
        try {
            final String sql = "SELECT id FROM ways_vertices_pgr ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326) LIMIT 1;";

            try (Connection connection = dataSource.getConnection()) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {

                    statement.setDouble(1, point.lon);
                    statement.setDouble(2, point.lat);

                    try (ResultSet rs = statement.executeQuery()) {
                        if (rs.next()) {
                            long vertexId = rs.getLong("id");

                            return OptionalLong.of(vertexId);
                        }
                    }
                }
            }

            return OptionalLong.empty();
        } catch (Exception e) {
            // TODO Add Logging to understand, why we cannot determine a Vertex.
            return OptionalLong.empty();
        }
    }

    public List<SimulatedRouteSegment> getRouteSegmentsBetween(Point start, Point end) throws Exception {

        OptionalLong sourceVertexId = getVertexId(start);

        if(sourceVertexId.isEmpty()) {
            // TODO Add Logging to understand why the Route is empty
            return new ArrayList<>();
        }

        OptionalLong targetVertexId = getVertexId(end);

        if(targetVertexId.isEmpty()) {
            // TODO Add Logging to understand why the Route is empty
            return new ArrayList<>();
        }

        return getRouteSegmentsBetweenVertex(sourceVertexId.getAsLong(), targetVertexId.getAsLong());
    }

    private List<SimulatedRouteSegment> getRouteSegmentsBetweenVertex(long sourceVertexId, long targetVertexId) throws Exception {
        ArrayList<SimulatedRouteSegment> routeSegments = new ArrayList<>();

        int segmentIndex = 0;

        // Execute the pgRouting Dijkstra query and get individual segment
        // details. 'maxspeed_forward' is typically created by osm2pgrouting.
        String sql = "SELECT " +
                "    route.seq, " +
                "    ways.osm_id AS way_id, " + // Changed ways.id to ways.osm_id for the original OSM Way ID
                "    COALESCE(ways.maxspeed_forward, 0) AS segment_speed_limit, " + // Prioritize maxspeed_forward, then maxspeed, else 0
                "    ST_AsText(ways.the_geom) AS way_geom_wkt " +
                "FROM pgr_dijkstra(" +
                "    'SELECT gid AS id, source, target, cost, reverse_cost FROM ways'," + // Changed id to gid AS id for the pgr_dijkstra subquery
                "    ?, ?," + // source_id, target_id
                "    directed := true" + // Assuming your osm2pgrouting data has proper directionality
                ") AS route " +
                "JOIN ways ON route.edge = ways.gid " + // Corrected: changed ways.id to ways.gid
                "WHERE route.edge != -1 " + // Exclude initial dummy edge/node if it exists
                "ORDER BY route.seq";

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setLong(1, sourceVertexId);
                statement.setLong(2, targetVertexId);

                try (ResultSet rs = statement.executeQuery()) {
                    // Regex to parse coordinates from LINESTRING WKT
                    // Matches pairs of floating-point numbers (longitude latitude)
                    Pattern pattern = Pattern.compile("(-?\\d+\\.\\d+)\\s+(-?\\d+\\.\\d+)");

                    while (rs.next()) {
                        String wayGeomWkt = rs.getString("way_geom_wkt");
                        double speedLimit = rs.getDouble("segment_speed_limit");

                        // Handle cases where maxspeed might be 0, null, or unreasonable
                        if (speedLimit <= 0 || Double.isNaN(speedLimit)) {
                            speedLimit = 50.0; // Fallback to a reasonable default
                        }

                        // Parse the LINESTRING WKT for this individual way's geometry
                        List<Point> segmentPoints = new ArrayList<>();

                        Matcher matcher = pattern.matcher(wayGeomWkt);
                        while (matcher.find()) {
                            // WKT is typically (longitude latitude), but our Point class is (latitude, longitude)
                            double lon = Double.parseDouble(matcher.group(1));
                            double lat = Double.parseDouble(matcher.group(2));
                            segmentPoints.add(new Point(lat, lon));
                        }

                        if (segmentPoints.size() >= 2) {
                            // Create a SimulatedRouteSegment using the actual start/end points of this way's geometry
                            // and its retrieved speed limit.
                            routeSegments.add(new SimulatedRouteSegment(
                                    segmentIndex,
                                    segmentPoints.get(0), // First point of the way's geometry
                                    segmentPoints.get(segmentPoints.size() - 1), // Last point of the way's geometry
                                    speedLimit
                            ));

                            segmentIndex++;
                        }
                    }
                }
            }
        }

        if (routeSegments.isEmpty()) {
            System.out.println("No route segments found for the given points. Check if points are connected and within OSM data bounds.");
        }

        return routeSegments;
    }
}