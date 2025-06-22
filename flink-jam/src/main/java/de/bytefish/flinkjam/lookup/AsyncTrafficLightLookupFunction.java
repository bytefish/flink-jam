package de.bytefish.flinkjam.lookup;

import de.bytefish.flinkjam.models.FullyEnrichedTrafficEvent;
import de.bytefish.flinkjam.models.RoadEnrichedTrafficEvent;
import de.bytefish.flinkjam.models.TrafficLightInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncTrafficLightLookupFunction extends RichAsyncFunction<RoadEnrichedTrafficEvent, FullyEnrichedTrafficEvent> {

    private transient ExecutorService executorService;

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final double lookupRadiusMeters;

    private static final String LOOKUP_SQL =
            "SELECT osm_id, name, ST_Y(geom) AS latitude, ST_X(geom) AS longitude, is_pedestrian_crossing_light, " +
                    "ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS distance_meters " +
                    "FROM traffic_lights " +
                    "WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography, ?) " + // ? = radius in meters
                    "ORDER BY distance_meters " +
                    "LIMIT 3;"; // Limit to top N closest lights

    public AsyncTrafficLightLookupFunction(String dbUrl, String dbUser, String dbPassword, double lookupRadiusMeters) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.lookupRadiusMeters = lookupRadiusMeters;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = Executors.newFixedThreadPool(10); // Use a separate pool or a shared one for all lookups
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public void asyncInvoke(RoadEnrichedTrafficEvent input, ResultFuture<FullyEnrichedTrafficEvent> resultFuture) throws Exception {
        executorService.submit(() -> {
            List<TrafficLightInfo> nearbyTrafficLights = new ArrayList<>();
            // Open connection within the submitted task to ensure thread safety
            try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                try (PreparedStatement statement = connection.prepareStatement(LOOKUP_SQL)) {
                    statement.setDouble(1, input.longitude); // For ST_MakePoint
                    statement.setDouble(2, input.latitude);  // For ST_MakePoint
                    statement.setDouble(3, input.longitude); // For ST_DWithin
                    statement.setDouble(4, input.latitude);  // For ST_DWithin
                    statement.setDouble(5, lookupRadiusMeters); // Radius in meters

                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            long id = rs.getLong("osm_id");
                            String name = rs.getString("name");
                            double lat = rs.getDouble("latitude");
                            double lon = rs.getDouble("longitude");
                            boolean isPedestrian = rs.getBoolean("is_pedestrian_crossing_light");
                            double distance = rs.getDouble("distance_meters");

                            nearbyTrafficLights.add(new TrafficLightInfo(id, name, lat, lon, isPedestrian, distance));
                        }
                    }
                }
                FullyEnrichedTrafficEvent outputEvent = new FullyEnrichedTrafficEvent(input, nearbyTrafficLights);
                resultFuture.complete(Collections.singleton(outputEvent));
            } catch (Exception e) {
                System.err.println("SQL Error during async traffic light lookup for event " + input.vehicleId + ": " + e.getMessage());
                resultFuture.completeExceptionally(e);
            }
        });
    }
}