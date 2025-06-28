package de.bytefish.flinkjam.lookup;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import java.util.concurrent.TimeUnit;

/**
 * An Asynchronous Lookup Function for enriching RawTrafficEvents with the road_segment data.
 */
public class AsyncRoadSegmentLookupFunction extends RichAsyncFunction<RawTrafficEvent, RoadEnrichedTrafficEvent> {

    private transient ExecutorService executorService;
    private transient HikariDataSource dataSource; // HikariCP DataSource

    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private double lookupRadiusMeters;

    private static final String LOOKUP_SQL =
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
                    "    distance asc";


    public AsyncRoadSegmentLookupFunction(String dbUrl, String dbUser, String dbPassword, double lookupRadiusMeters) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.lookupRadiusMeters = lookupRadiusMeters;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize HikariCP connection pool
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMinimumIdle(5); // Minimum idle connections
        config.setMaximumPoolSize(20); // Maximum total connections (tune this based on DB capacity)
        config.setConnectionTimeout(5000); // 5 seconds connection timeout
        config.setIdleTimeout(300000); // 5 minutes idle timeout
        config.setMaxLifetime(1800000); // 30 minutes max connection lifetime

        // Adjust the thread pool size for the AsyncFunction to match or exceed HikariCP's pool size
        // A higher number of threads allows more concurrent async DB calls.
        executorService = Executors.newFixedThreadPool(config.getMaximumPoolSize() * 2);

        dataSource = new HikariDataSource(config);
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS); // Wait for threads to finish
        }

        if (dataSource != null) {
            dataSource.close(); // Close the connection pool
        }
    }

    @Override
    public void asyncInvoke(RawTrafficEvent input, ResultFuture<RoadEnrichedTrafficEvent> resultFuture) throws Exception {
        executorService.submit(() -> {
            RoadEnrichedTrafficEvent outputEvent = null;
            try (Connection connection = dataSource.getConnection()) {
                try (PreparedStatement statement = connection.prepareStatement(LOOKUP_SQL)) {

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