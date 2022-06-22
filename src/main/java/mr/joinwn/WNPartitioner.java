package mr.joinwn;

import common.TriplesDBKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class WNPartitioner extends Partitioner<TriplesDBKey, Object> {
    @Override
    public int getPartition(TriplesDBKey o, Object o2, int i) {
        return o.getStemmedK().hashCode() % i;
    }
}
