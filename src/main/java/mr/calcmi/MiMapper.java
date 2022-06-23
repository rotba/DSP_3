package mr.calcmi;

import mr.common.TriplesDBKey;
import mr.common.TriplesDBValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MiMapper extends Mapper<TriplesDBKey, TriplesDBValue,TriplesDBKey, TriplesDBValue> {
    private static final Logger logger = Logger.getLogger(MiMapper.class);
    static final boolean LOCAL = false;
    @Override
    protected void map(TriplesDBKey key, TriplesDBValue value, Mapper<TriplesDBKey, TriplesDBValue, TriplesDBKey, TriplesDBValue>.Context context) throws IOException, InterruptedException {
        if(LOCAL) logger.info(String.format("key:%s", key.toString()));
        key.setJoinPn(true);
        context.write(key,value);
    }
}
