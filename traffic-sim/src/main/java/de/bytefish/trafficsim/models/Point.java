package de.bytefish.trafficsim.models;

/**
 * Represents a geographical point with latitude and longitude.
 */
public class Point {
    public double lat;
    public double lon;

    public Point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "(" + lat + ", " + lon + ")";
    }
}
