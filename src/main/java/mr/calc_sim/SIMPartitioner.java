package mr.calc_sim;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SIMPartitioner extends Partitioner<Text, Object> {
    @Override
    public int getPartition(Text text, Object o, int i) {
        return Math.abs(text.toString().hashCode())%i;
    }
}
