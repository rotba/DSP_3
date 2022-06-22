package mr.joinwn;

import common.TriplesDBKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WNCCombiner extends Reducer<TriplesDBKey, IntWritable, TriplesDBKey, IntWritable> {
    @Override
    protected void reduce(TriplesDBKey key, Iterable<IntWritable> values, Reducer<TriplesDBKey, IntWritable, TriplesDBKey, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val :
                values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
