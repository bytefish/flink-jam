package de.bytefish.flinkjam.models;

import java.util.Objects;

/**
 * Represents information about a single nearby traffic light.
 */
public class TrafficLightInfo {
    public long osmId;
    public String name;
    public double latitude;
    public double longitude;
    public boolean isPedestrianCrossingLight;
    public double distanceMeters; // Distance from the vehicle to this traffic light

    public TrafficLightInfo() {}

    public TrafficLightInfo(long osmId, String name, double latitude, double longitude, boolean isPedestrianCrossingLight, double distanceMeters) {
        this.osmId = osmId;
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
        this.isPedestrianCrossingLight = isPedestrianCrossingLight;
        this.distanceMeters = distanceMeters;
    }

    // Override equals and hashCode for proper comparison in Set
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrafficLightInfo that = (TrafficLightInfo) o;
        return osmId == that.osmId; // Only compare by ID for uniqueness
    }

    @Override
    public int hashCode() {
        return Objects.hash(osmId); // Only hash by ID for uniqueness
    }

    @Override
    public String toString() {
        return "TrafficLightInfo{" +
                "id=" + osmId +
                ", name='" + (name != null ? name : "N/A") + '\'' +
                ", lat=" + latitude +
                ", lon=" + longitude +
                ", isPedestrian=" + isPedestrianCrossingLight +
                ", dist=" + String.format("%.2f", distanceMeters) + "m" +
                '}';
    }
}