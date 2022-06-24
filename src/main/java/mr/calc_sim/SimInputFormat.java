package mr.calc_sim;


import mr.common.TriplesDBKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class SimInputFormat extends FileInputFormat<TriplesDBKey, DoubleWritable> {
    @Override
    public RecordReader<TriplesDBKey, DoubleWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SimRecordReader();
    }
}
