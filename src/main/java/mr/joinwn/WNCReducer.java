package mr.joinwn;

import common.TriplesDBValue;
import common.TriplesDBKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WNCReducer extends Reducer<TriplesDBKey, IntWritable, TriplesDBKey, TriplesDBValue> {
    public enum ReducerCounters{
        SLOT_COUNT,
        XSLOT_TOTAL,
        YSLOT_TOTAL,
        P_COUNT,
        W_COUNT,
        K_COUNT,
    };
    private int wn;

    @Override
    protected void reduce(TriplesDBKey key, Iterable<IntWritable> values, Reducer<TriplesDBKey, IntWritable, TriplesDBKey, TriplesDBValue>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val :
                values) {
            sum += val.get();
        }
        if(key.getStemmedK().equals(TriplesDBKey.STAR) && key.getW().equals(TriplesDBKey.STAR)){
            context.getConfiguration().set("SLOT"+key.getSlot(), String.valueOf(sum));
            context.getConfiguration().reloadConfiguration();
            context.getCounter(ReducerCounters.SLOT_COUNT).increment(1);
            if(key.getSlot().equals(TriplesDBKey.X)){
                context.getCounter(ReducerCounters.XSLOT_TOTAL).increment(sum);
            }else{
                context.getCounter(ReducerCounters.YSLOT_TOTAL).increment(sum);
            }
        } else if (key.getW().equals(TriplesDBKey.STAR)) {
            context.write(key, new TriplesDBValue(-1, sum, -1,-1));
            context.getCounter(ReducerCounters.P_COUNT).increment(1);
        } else if (key.getStemmedK().equals(TriplesDBKey.STAR)) {
            wn = sum;
            context.getCounter(ReducerCounters.W_COUNT).increment(1);
        }else{
            context.getCounter(ReducerCounters.K_COUNT).increment(1);
            context.write(key, new TriplesDBValue(-1, -1, wn, sum));
        }
    }
}
