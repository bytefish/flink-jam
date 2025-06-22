package de.bytefish.flinkjam.lookup;

import de.bytefish.flinkjam.models.RawTrafficEvent;
import de.bytefish.flinkjam.models.RoadEnrichedTrafficEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An Asynchronous Lookup Function for enriching RawTrafficEvents with the road_segment data.
 */
public class AsyncRoadSegmentLookupFunction extends RichAsyncFunction<RawTrafficEvent, RoadEnrichedTrafficEvent> {

    private transient ExecutorService executorService;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final double lookupRadiusMeters;

    public AsyncRoadSegmentLookupFunction(String dbUrl, String dbUser, String dbPassword, double lookupRadiusMeters) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.lookupRadiusMeters = lookupRadiusMeters;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = Executors.newFixedThreadPool(10); // Thread pool for concurrent DB lookups
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public void asyncInvoke(RawTrafficEvent input, ResultFuture<RoadEnrichedTrafficEvent> resultFuture) throws Exception {
        executorService.submit(() -> {
            RoadEnrichedTrafficEvent outputEvent = null;
            try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT\n" +
                                "    osm_id, \n" +
                                "    osm_type,\n" +
                                "    road_type,\n" +
                                "    speed_limit_kmh,\n" +
                                "    name,\n" +
                                "    geom <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography as distance\n" +
                                "FROM\n" +
                                "    road_segments\n" +
                                "WHERE \n" +
                                "    ST_DWithin(geom, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography, ?)\n" +
                                "ORDER BY \n" +
                                "    distance asc")) {

                    statement.setDouble(1, input.longitude);
                    statement.setDouble(2, input.latitude);
                    statement.setDouble(3, input.longitude);
                    statement.setDouble(4, input.latitude);
                    statement.setDouble(5, lookupRadiusMeters);

                    try (ResultSet rs = statement.executeQuery()) {
                        if (rs.next()) {
                            String roadSegmentId = rs.getString("osm_id");
                            String roadType = rs.getString("road_type");
                            int speedLimit = rs.getInt("speed_limit_kmh");
                            outputEvent = new RoadEnrichedTrafficEvent(input, roadSegmentId, speedLimit, roadType);
                        } else {
                            // Important: Complete the future even if no segment is found
                            System.out.println("No road segment found for " + input.latitude + ", " + input.longitude + " - " + input.vehicleId);
                            outputEvent = new RoadEnrichedTrafficEvent(input, null, 0, "Unknown");
                        }
                    }
                }
                resultFuture.complete(Collections.singleton(outputEvent));
            } catch (Exception e) { // Catch any exception during lookup (e.g., SQLException)
                System.err.println("Error during async road segment lookup for event " + input.vehicleId + ": " + e.getMessage());
                resultFuture.completeExceptionally(e); // Ensure future is completed exceptionally
            }
        });
    }
}