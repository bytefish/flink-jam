package de.bytefish.flinkjam.models;

import java.util.List;
import java.util.Objects;

/**
 * Represents a fully enriched traffic event, including road segment and nearby traffic light data.
 * This is the event used for CEP.
 */
public class FullyEnrichedTrafficEvent {
    public long timestamp;
    public String vehicleId;
    public double latitude;
    public double longitude;
    public double speed;
    public String roadSegmentId;
    public int speedLimitKmh;
    public String roadType;
    public List<TrafficLightInfo> nearbyTrafficLights; // List of nearby traffic lights

    public FullyEnrichedTrafficEvent() {
    }

    public FullyEnrichedTrafficEvent(RoadEnrichedTrafficEvent roadEvent, List<TrafficLightInfo> nearbyTrafficLights) {
        this.timestamp = roadEvent.timestamp;
        this.vehicleId = roadEvent.vehicleId;
        this.latitude = roadEvent.latitude;
        this.longitude = roadEvent.longitude;
        this.speed = roadEvent.speed;
        this.roadSegmentId = roadEvent.roadSegmentId;
        this.speedLimitKmh = roadEvent.speedLimitKmh;
        this.roadType = roadEvent.roadType;
        this.nearbyTrafficLights = nearbyTrafficLights;
    }

    // Getters
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

    public List<TrafficLightInfo> getNearbyTrafficLights() {
        return nearbyTrafficLights;
    }


    @Override
    public String toString() {
        return "FullyEnrichedTrafficEvent{" +
                "timestamp=" + timestamp +
                ", vehicleId='" + vehicleId + '\'' +
                ", roadSegmentId='" + roadSegmentId + '\'' +
                ", speed=" + speed +
                ", speedLimit=" + speedLimitKmh +
                ", roadType='" + roadType + '\'' +
                ", nearbyLights=" + nearbyTrafficLights +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullyEnrichedTrafficEvent that = (FullyEnrichedTrafficEvent) o;
        return timestamp == that.timestamp &&
                Double.compare(that.speed, speed) == 0 &&
                Double.compare(that.latitude, latitude) == 0 &&
                Double.compare(that.longitude, longitude) == 0 &&
                speedLimitKmh == that.speedLimitKmh &&
                Objects.equals(vehicleId, that.vehicleId) &&
                Objects.equals(roadSegmentId, that.roadSegmentId) &&
                Objects.equals(roadType, that.roadType) &&
                Objects.equals(nearbyTrafficLights, that.nearbyTrafficLights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, vehicleId, latitude, longitude, speed, roadSegmentId, speedLimitKmh, roadType, nearbyTrafficLights);
    }
}
