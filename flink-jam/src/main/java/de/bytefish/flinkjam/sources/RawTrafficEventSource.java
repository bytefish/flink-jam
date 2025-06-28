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

    public List<RawTrafficEvent> simulatedEvents;

    public long currentBaseTimestamp;

    public RawTrafficEventSource() {
        // Initialize the base simulated events once
        simulatedEvents = new ArrayList<>();

        // Populate initial simulated events
        currentBaseTimestamp = System.currentTimeMillis();

        // Simulate normal traffic on A1 initially
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp, "Car001", 52.194443, 7.797556, 90));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp, "Car002", 52.194512, 7.797632, 95));

        // Simulate Light Slowdown on A1 (Exit FMO Airport) (e.g., speed below 70%, 3+ events, 2+ unique vehicles)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 5000, "Car001", 52.115703, 7.702582, 65));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 10000, "Car002", 52.115849, 7.702784, 60));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 15000, "Car003", 52.115611, 7.702527, 68));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 20000, "Car001", 52.116178, 7.703204, 62));

        // Simulate Sustained Slowdown on A1 (e.g., speed below 50%, 5+ events, 3+ unique vehicles)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 25000, "Car008", 52.041431, 7.607906, 45));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 30000, "Car009", 52.041487, 7.607935, 40));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 35000, "Car010", 52.041200, 7.607768, 42));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 40000, "Car008", 52.041744, 7.608094, 35));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 45000, "Car009", 52.041499, 7.608008, 38));

        // Simulate Traffic Jam (e.g., speed below 10 km/h, 7+ events, 5+ unique vehicles)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 50000, "TruckX", 52.006419, 7.575624, 8));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 53000, "CarY", 52.006460, 7.575661, 5));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 56000, "BusZ", 52.006511, 7.575714, 3));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 59000, "MotoA", 52.006555, 7.575762, 1));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 62000, "VanB", 52.006612, 7.575814, 7));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 65000, "TruckX", 52.006687, 7.575893, 2));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 68000, "CarY", 52.006760, 7.575959, 4));

        // Simulate a scenario that's just a long red light (high density, low speed, but expected)
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 78000, "CarD", 51.966097, 7.623803, 2));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 81000, "CarE", 51.966113, 7.623785, 0));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 84000, "CarF", 51.966138, 7.623783, 1));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 87000, "CarG", 51.966162, 7.623780, 0));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 90000, "CarH", 51.966184, 7.623771, 3));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 93000, "CarI", 51.966092, 7.623775, 0));
        simulatedEvents.add(new RawTrafficEvent(currentBaseTimestamp + 96000, "CarJ", 51.966217, 7.623759, 2));
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
                    currentBaseTimestamp + (originalEvent.timestamp - simulatedEvents.get(0).timestamp),
                    originalEvent.vehicleId,
                    originalEvent.latitude,
                    originalEvent.longitude,
                    originalEvent.speed
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