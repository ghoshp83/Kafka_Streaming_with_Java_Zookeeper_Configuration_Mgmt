package com.pralay.LoadHDFS;

/**
 * This class represents a generic queue item where each item is
 * represented as time stamp and Object.
 */
@Deprecated
public class QueueItem {

    public final long timestamp;
    public final Object item;
    public final long offset; //this is the offset into the Kafka topic+partition

    public QueueItem(long timestamp, Object item, long offset) {
        this.timestamp = timestamp;
        this.item = item;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "event: " + timestamp + " " + item;
    }

}
