package de.bytefish.trafficsim.models;

import de.bytefish.trafficsim.utils.Haversine;

/**
 * Represents a small, discrete segment within the overall simulated route,
 * derived from the sequence of points returned by pgRouting. Each segment
 * now carries its specific speed limit as extracted from OSM data via pgRouting.
 */
public class SimulatedRouteSegment {
    int index; // Unique identifier for this micro-segment within the route (0 to N-1)
    Point start;
    Point end;
    double lengthKm;
    double speedLimitKmh; // Actual speed limit for this segment, from OSM data
    double currentCongestionFactor; // Factor applied to speed limit (1.0 = no congestion)

    public SimulatedRouteSegment(int index, Point start, Point end, double speedLimitKmh) {
        this.index = index;
        this.start = start;
        this.end = end;
        this.speedLimitKmh = speedLimitKmh;
        this.lengthKm = Haversine.calculateDistance(start.lat, start.lon, end.lat, end.lon);
        // Ensure minimum length to avoid division by zero or very large speed increments
        if (this.lengthKm < 0.0001) { // Min 0.1 meter
            this.lengthKm = 0.0001;
        }
        this.currentCongestionFactor = 1.0; // Default to no congestion
    }
}