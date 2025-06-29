package de.bytefish.trafficsim.models;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Represents a single recorded traffic event (a row in the output CSV).
 */
public class TrafficEventRecord {
    private final LocalDateTime timestamp;
    private final String vehicleId;
    private final double latitude;
    private final double longitude;
    private final double speedKmh;
    private final int simulatedRouteSegmentIndex;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TrafficEventRecord(LocalDateTime timestamp, String vehicleId, double latitude, double longitude,
                              double speedKmh, int simulatedRouteSegmentIndex) {
        this.timestamp = timestamp;
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speedKmh = speedKmh;
        this.simulatedRouteSegmentIndex = simulatedRouteSegmentIndex;
    }

    /**
     * Converts the record to a CSV formatted string.
     */
    public String toCsvString() {
        return String.format(Locale.US, "%s,%s,%.6f,%.6f,%.1f,%d",
                timestamp.format(FORMATTER),
                vehicleId,
                latitude,
                longitude,
                speedKmh,
                simulatedRouteSegmentIndex
        );
    }
}