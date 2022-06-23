package mr.common;

import mr.common.TriplesDBKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class TBDKeyPartitioner extends Partitioner<TriplesDBKey, Object> {
    @Override
    public int getPartition(TriplesDBKey o, Object o2, int i)
    {
        if(o.isJoinPn()){
            return Math.abs(o.getStemmedK().hashCode()) % i;
        }
        if(o.getStemmedK().equals(TriplesDBKey.STAR) && o.getW().equals(TriplesDBKey.STAR)){
            return o.getSlot().equals(TriplesDBKey.X) ? 0:1;
        }
        return Math.abs(o.getW().hashCode())%i;
    }
}
