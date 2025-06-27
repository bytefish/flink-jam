package de.bytefish.flinkjam.models;

import java.util.List;

/**
 * Represents a detected congestion warning.
 */
public class CongestionWarning implements java.io.Serializable  {
    public long timestamp;
    public String warningType;
    public String roadSegmentId;
    public double currentAverageSpeed;
    public int speedLimit;
    public String details;
    public List<TrafficLightInfo> relatedTrafficLights; // Can include lights related to the congestion
    public int affectedUniqueVehiclesCount; // New field

    public CongestionWarning() {
    }

    public CongestionWarning(long timestamp, String roadSegmentId, String warningType, double currentAverageSpeed, int speedLimit, String details, List<TrafficLightInfo> relatedTrafficLights, int affectedUniqueVehiclesCount) {
        this.timestamp = timestamp;
        this.roadSegmentId = roadSegmentId;
        this.warningType = warningType;
        this.currentAverageSpeed = currentAverageSpeed;
        this.speedLimit = speedLimit;
        this.details = details;
        this.relatedTrafficLights = relatedTrafficLights;
        this.affectedUniqueVehiclesCount = affectedUniqueVehiclesCount;
    }

    @Override
    public String toString() {
        return "!!! CONGESTION ALERT !!! " +
                "Timestamp=" + timestamp +
                ", RoadSegmentId='" + roadSegmentId + '\'' +
                ", Type='" + warningType + '\'' +
                ", AvgSpeed=" + String.format("%.2f", currentAverageSpeed) + " km/h" +
                ", SpeedLimit=" + speedLimit + " km/h" +
                ", Details='" + details + '\'' +
                ", UniqueVehicles=" + affectedUniqueVehiclesCount +
                ", RelatedLights=" + relatedTrafficLights;
    }
}