package de.bytefish.trafficsim.services;

import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.trafficsim.models.Point;
import de.bytefish.trafficsim.models.SimulatedRouteSegment;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PgRoutingService {

    private final DataSource dataSource;

    public PgRoutingService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Gets the nearest Vertex for a given point.
     *
     * @param latitude Latitude
     * @param longitude Longitude
     * @return Get the Vertex for the given Point
     * @throws Exception Any Exception thrown during Processing
     */
    public OptionalLong getVertexIdByLatitudeAndLongitude(double latitude, double longitude) throws Exception {
        final String sql = "SELECT id FROM ways_vertices_pgr ORDER BY the_geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326) LIMIT 1;";

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setDouble(1, longitude);
                statement.setDouble(2, latitude);

                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        long vertexId = rs.getLong("id");

                        return OptionalLong.of(vertexId) ;
                    }
                }
            }
        }

        return OptionalLong.empty();
    }

    public ArrayList<SimulatedRouteSegment> getRouteSegmentsByCoordinates(double startLat, double startLon, double endLat, double endLon) throws Exception {
        OptionalLong sourceVertexId = getVertexIdByLatitudeAndLongitude(startLat, startLon);

        if(sourceVertexId.isEmpty()) {
            // TODO Logging
            return new ArrayList<>();
        }

        OptionalLong targetVertexId = getVertexIdByLatitudeAndLongitude(endLat, endLon);

        if(targetVertexId.isEmpty()) {
            // TODO Logging
            return new ArrayList<>();
        }

        return getRouteSegmentsByVertex(sourceVertexId.getAsLong(), targetVertexId.getAsLong());
    }

    public ArrayList<SimulatedRouteSegment> getRouteSegmentsByVertex(long sourceVertexId, long targetVertexId) throws Exception {
        ArrayList<SimulatedRouteSegment> routeSegments = new ArrayList<>();

        int segmentIndex = 0;

        // Execute the pgRouting Dijkstra query and get individual segment
        // details. 'maxspeed_forward' is typically created by osm2pgrouting.
        String sql = "SELECT " +
                        "    route.seq, " +
                        "    ways.id AS way_id, " +
                        "    COALESCE(ways.maxspeed_forward, ways.maxspeed, 0) AS segment_speed_limit, " + // Prioritize maxspeed_forward, then maxspeed, else 0
                        "    ST_AsText(ways.geom_way) AS way_geom_wkt " +
                        "FROM pgr_dijkstra(" +
                        "    'SELECT id, source, target, cost, reverse_cost FROM ways'," + // Include reverse_cost for directed graph
                        "    ?, ?," + // source_vertex_id, target_vertex_id
                        "    directed := true" +
                        ") AS route " +
                        "JOIN ways ON route.edge = ways.id " +
                        "WHERE route.edge != -1 " +
                        "ORDER BY route.seq;";

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setDouble(1, sourceVertexId);
                statement.setDouble(2, targetVertexId);

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