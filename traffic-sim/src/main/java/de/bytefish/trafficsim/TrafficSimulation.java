package de.bytefish.trafficsim;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.trafficsim.models.CongestionEvent;
import de.bytefish.trafficsim.models.Point;
import de.bytefish.trafficsim.models.SimulatedRouteSegment;
import de.bytefish.trafficsim.models.TrafficEventRecord;
import de.bytefish.trafficsim.services.PgRoutingService;
import de.bytefish.trafficsim.simulation.TrafficSimulator;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

public class TrafficSimulationApplication {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/flinkjam";
    private static final String DB_USER = "postgis";
    private static final String DB_PASSWORD = "postgis";

    public static void main(String[] args) throws Exception {

        // Create Data Source for the PostGIS database
        DataSource dataSource = getHikariDataSource(DB_URL, DB_USER, DB_PASSWORD);

        // The Routing Service to be used for querying the PostGIS database
        PgRoutingService pgRoutingService = new PgRoutingService(dataSource);

        // This is the Start and End Position for our Route. This should probably be an Autobahn as a
        // first guess. If we are using too many cars, there might be a horrible build-up at
        // intersections, that will automatically lead to warnings.
        Point startPoint = new Point(52.248952, 7.909707);
        Point endPoint = new Point(51.981584, 7.552554);

        // Now we create the Route Segments between both Points. This returns a list of SimulatedRoadSegments,
        // which will be used to map the Vehicles to.
        List<SimulatedRouteSegment> simulatedRouteSegments = pgRoutingService.getRouteSegmentsBetween(startPoint, endPoint);

        // We want to simulate CongestionEvents on the route, that define the start time, the end time and
        // the position on the simulated road the congestion occurs upon.
        List<CongestionEvent> congestionEvents = createCongestionEvents(simulatedRouteSegments);

        // Builds a new TrafficSimulator we are going to derive the vehicle data from. In this example we are simulating
        // 40 vehicles with a maximum duration of 60 minutes, with the vehicles sending in a 3 second interval.
        TrafficSimulator trafficSimulator = new TrafficSimulator(40, 60, 3, 5, 0.8);

        // Now run the Simulation based on the Route Segments determined by pgRouting and our simulated congestion events. The idea
        // is, that the Apache Flink Pipeline needs to detect exactely these simulated data points. We are not using the processing
        // time, but use the event time in Apache Flink, so putting in the events faster, than in real life is not a problem.
        List<TrafficEventRecord> trafficEventRecords = trafficSimulator.runSimulationLogic(simulatedRouteSegments, congestionEvents);

        // For now just print out the vehicle traffic data as CSV.
        for(TrafficEventRecord trafficEventRecord : trafficEventRecords) {
            System.out.println(trafficEventRecord.toCsvString());
        }
    }

    public static List<CongestionEvent> createCongestionEvents(List<SimulatedRouteSegment> simulatedRouteSegments) {
        List<CongestionEvent> congestionEvents = new ArrayList<>();

        // Example 1: Slowdown in the first third of the route
        congestionEvents.add(new CongestionEvent(
                (int)(simulatedRouteSegments.size() * 0.1),
                (int)(simulatedRouteSegments.size() * 0.3),
                20, 45, 0.4)); // 40% speed limit for 25 min

        // Example 2: Heavy jam in the middle of the route
        congestionEvents.add(new CongestionEvent(
                (int)(simulatedRouteSegments.size() * 0.4),
                (int)(simulatedRouteSegments.size() * 0.6),
                50, 70, 0.15)); // 15% speed limit for 20 min

        return congestionEvents;
    }

    public static DataSource getHikariDataSource(String dbUrl, String dbUser, String dbPassword) {
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

        return new HikariDataSource(config);
    }
}
