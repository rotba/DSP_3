package mr.common;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class TDBKInputFormat extends FileInputFormat<TriplesDBKey, TriplesDBValue> {
    @Override
    public RecordReader<TriplesDBKey, TriplesDBValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TDBRecordReader();
    }
}
