package de.bytefish.flinkjam.sources;

import de.bytefish.flinkjam.models.RawTrafficEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * A custom Flink SourceFunction to continuously generate simulated RawTrafficEvents.
 * This will prevent the Flink job from terminating due to source exhaustion.
 */
public class RawTrafficEventSource implements SourceFunction<RawTrafficEvent> {
    private volatile boolean isRunning = true;
    private final List<RawTrafficEvent> simulatedEvents;
    private long currentBaseTimestamp;

    public RawTrafficEventSource() {
        // Initialize the base simulated events once
        simulatedEvents = new ArrayList<>();

        // Populate initial simulated events
        currentBaseTimestamp = System.currentTimeMillis();

        // Simulate normal traffic on A1 initially
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp, "Car001", 51.0001, 7.0001, 90));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp, "Car002", 51.0002, 7.0002, 95));

        // Simulate Light Slowdown on A1 (e.g., speed below 70%, 3+ events, 2+ unique vehicles)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 5000, "Car001", 51.0003, 7.0003, 65));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 10000, "Car002", 51.0004, 7.0004, 60));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 15000, "Car003", 51.0005, 7.0005, 68));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 20000, "Car001", 51.0006, 7.0006, 62));

        // Simulate Sustained Slowdown on A1 (e.g., speed below 50%, 5+ events, 3+ unique vehicles)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 25000, "Car008", 51.0010, 7.0010, 45));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 30000, "Car009", 51.0011, 7.0011, 40));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 35000, "Car010", 51.0012, 7.0012, 42));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 40000, "Car008", 51.0013, 7.0013, 35));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 45000, "Car009", 51.0014, 7.0014, 38));

        // Simulate Traffic Jam (e.g., speed below 10 km/h, 7+ events, 5+ unique vehicles)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 50000, "TruckX", 51.2000, 7.5000, 8));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 53000, "CarY", 51.2001, 7.5001, 5));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 56000, "BusZ", 51.2002, 7.5002, 3));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 59000, "MotoA", 51.2003, 7.5003, 1));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 62000, "VanB", 51.2004, 7.5004, 7));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 65000, "TruckX", 51.2005, 7.5005, 2));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 68000, "CarY", 51.2006, 7.5006, 4));

        // Simulate a scenario that's just a long red light (high density, low speed, but expected)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 78000, "CarD", 51.2000, 7.5000, 2));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 81000, "CarE", 51.2001, 7.5001, 0));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 84000, "CarF", 51.2002, 7.5002, 1));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 87000, "CarG", 51.2003, 7.5003, 0));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 90000, "CarH", 51.2004, 7.5004, 3));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 93000, "CarI", 51.2005, 7.5005, 0));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 96000, "CarJ", 51.2006, 7.5006, 2));
    }

    @Override
    public void run(SourceContext<RawTrafficEvent> ctx) throws Exception {
        int eventIndex = 0;
        // Define a delay between each event emission to simulate real-time data
        long delayBetweenEventsMillis = 1000; // 1 second between events

        while (isRunning) {
            if (eventIndex >= simulatedEvents.size()) {
                // Loop back to the beginning of the simulated data
                eventIndex = 0;
                // Optionally adjust timestamps for the next loop to keep them increasing
                currentBaseTimestamp = System.currentTimeMillis();
            }

            RawTrafficEvent originalEvent = simulatedEvents.get(eventIndex);

            // Create a new event to update its timestamp
            RawTrafficEvent eventToSend = new RawTrafficEvent(
                    currentBaseTimestamp + (originalEvent.getTimestamp() - simulatedEvents.get(0).getTimestamp()),
                    originalEvent.getVehicleId(),
                    originalEvent.getLatitude(),
                    originalEvent.getLongitude(),
                    originalEvent.getSpeed()
            );

            ctx.collect(eventToSend);
            eventIndex++;

            try {
                Thread.sleep(delayBetweenEventsMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isRunning = false; // Stop if interrupted
                System.err.println("RawTrafficEventSource interrupted during sleep.");
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}