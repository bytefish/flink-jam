package de.bytefish.flinkjam.models;

/**
 * Represents a raw real-time traffic event from a vehicle, before any enrichment.
 */
public class RawTrafficEvent implements java.io.Serializable {

    public long timestamp;
    public String vehicleId;
    public double latitude;
    public double longitude;
    public double speed;

    public RawTrafficEvent() {
    }

    public RawTrafficEvent(long timestamp, String vehicleId, double latitude, double longitude, double speed) {
        this.timestamp = timestamp;
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
    }

    // Getters
    public long getTimestamp() {
        return timestamp;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getSpeed() {
        return speed;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    @Override
    public String toString() {
        return "RawTrafficEvent{" +
                "timestamp=" + timestamp +
                ", vehicleId='" + vehicleId + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speed=" + speed +
                '}';
    }
}
