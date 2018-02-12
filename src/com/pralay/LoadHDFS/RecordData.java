package com.pralay.LoadHDFS;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import parquet.avro.AvroParquetWriter;

import java.util.HashMap;
import java.util.List;

public class RecordData {
    String eventType;
    DataFileWriter avroWriter;
    AvroParquetWriter parquetWriter;
    String openFileName;

    public String toString(){
        return("eventType="+eventType+"::openFileName="+openFileName);
    }
}
