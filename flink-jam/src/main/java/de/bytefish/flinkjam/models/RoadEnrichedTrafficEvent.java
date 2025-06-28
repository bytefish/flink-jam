package de.bytefish.flinkjam.models;


/**
 * Represents a traffic event enriched with road segment data.
 */
public class RoadEnrichedTrafficEvent implements java.io.Serializable  {
    public long timestamp;
    public String vehicleId;
    public double latitude;
    public double longitude;
    public double speed;
    public String roadSegmentId;
    public int speedLimitKmh;
    public String roadType;

    public RoadEnrichedTrafficEvent() {
    }

    public RoadEnrichedTrafficEvent(RawTrafficEvent rawEvent, String roadSegmentId, int speedLimitKmh, String roadType) {
        this.timestamp = rawEvent.timestamp;
        this.vehicleId = rawEvent.vehicleId;
        this.latitude = rawEvent.latitude;
        this.longitude = rawEvent.longitude;
        this.speed = rawEvent.speed;
        this.roadSegmentId = roadSegmentId;
        this.speedLimitKmh = speedLimitKmh;
        this.roadType = roadType;
    }

    // Getters (needed for Flink's POJO detection and keying)
    public long getTimestamp() {
        return timestamp;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public String getRoadSegmentId() {
        return roadSegmentId;
    }

    public double getSpeed() {
        return speed;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public int getSpeedLimitKmh() {
        return speedLimitKmh;
    }

    public String getRoadType() {
        return roadType;
    }

    @Override
    public String toString() {
        return "RoadEnrichedTrafficEvent{" +
                "timestamp=" + timestamp +
                ", vehicleId='" + vehicleId + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speed=" + speed +
                ", roadSegmentId='" + roadSegmentId + '\'' +
                ", speedLimitKmh=" + speedLimitKmh +
                ", roadType='" + roadType + '\'' +
                '}';
    }
}
