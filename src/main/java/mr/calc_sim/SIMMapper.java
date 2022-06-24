package mr.calc_sim;

import mr.common.TriplesDBKey;
import mr.common.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class SIMMapper extends Mapper<TriplesDBKey, DoubleWritable, Text, SimValue> {
    private String[][] testSet;

    @Override
    protected void setup(Mapper<TriplesDBKey, DoubleWritable, Text, SimValue>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        testSet = Utils.toArray(context.getConfiguration().get("TESTSET"));
    }

    @Override
    protected void map(TriplesDBKey key, DoubleWritable value, Mapper<TriplesDBKey, DoubleWritable, Text, SimValue>.Context context) throws IOException, InterruptedException {
        for (int i = 0; i < testSet.length; i++) {
            for (int j = 0; j < testSet[i].length; j++) {
                if(key.getNaturalK().equals(testSet[i][j])){
                    context.write(new Text(testSet[i][0]+"\t"+testSet[i][1]), new SimValue(key,value));
                }
            }
        }
    }
}
