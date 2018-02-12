package com.pralay.LoadHDFS;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionRecord {

    long startTime = -1L;
    int partitionId;
    String topicName;
    //Most probably one Schema per topic - however, its possible that objects with a different schema are published
    Map<Schema,RecordData> rawEventRecMap = new HashMap<Schema,RecordData>();
    int ctr;
    long offset;
    long firstTimeStamp;
    long endTimeStamp;
    int numRecsDropped;
    int numRecsPolled;

    public String toString(){
        return("partitionId="+partitionId+"::topic="+topicName+"::rawEventRecMap="+rawEventRecMap);
    }
}
