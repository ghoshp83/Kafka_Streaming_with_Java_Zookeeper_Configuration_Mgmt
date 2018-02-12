package com.pralay.LoadHDFS;

import java.util.Comparator;

/**
 * QueueItemComparator: compares QueueItem on ascending time stamp
 */
@Deprecated
public class QueueItemComparator implements Comparator<QueueItem> {
    @Override
    public int compare(QueueItem x, QueueItem y) {
        if (x.timestamp < y.timestamp) { return -1; }
        if (x.timestamp > y.timestamp) { return 1;  }
        return 0;
    }
}
