package mr.joinwn;

import googlebiarcs.SyntacticNgramHasNot4Parts;
import mr.calc_sim.SIMReducer;
import mr.common.TriplesDBKey;
import googlebiarcs.DependecyPath;
import googlebiarcs.ParsedBiarcs;
import mr.common.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class WNMapper extends Mapper<Object, Text, TriplesDBKey, IntWritable> {
    static final boolean LOCAL = false;
    private static final Logger logger = Logger.getLogger(WNMapper.class);
    public enum WNMapperCounters{
        SyntacticNgramHasNot4Parts
    }
    Map<String,String> testSet = new HashMap<>();
    @Override
    protected void setup(Mapper<Object, Text, TriplesDBKey, IntWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        testSet = Utils.toHashMap(context.getConfiguration().get("TESTSET"));
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, TriplesDBKey, IntWritable>.Context context) throws IOException, InterruptedException {
        if (LOCAL)logger.info(String.format("value:%s", value.toString()));
        ParsedBiarcs pb = null;
        try {
            pb = new ParsedBiarcs(value.toString());
        } catch (SyntacticNgramHasNot4Parts e) {
            context.getCounter(WNMapperCounters.SyntacticNgramHasNot4Parts).increment(1);
            return;
        }
        if(!pb.headIsVerb()){
            return;
        }
        List<DependecyPath> dps = pb.getDependencyPaths();
        for(DependecyPath dp: dps){
            if(dp.slotXTypeIsNoun()&& dp.slotYTypeIsNoun()){
                context.write(new TriplesDBKey(TriplesDBKey.STAR, TriplesDBKey.X, TriplesDBKey.STAR, "NO_NATURAL"), new IntWritable(pb.getCount()));
                context.write(new TriplesDBKey(TriplesDBKey.STAR, TriplesDBKey.X, dp.getX(), "NO_NATURAL"), new IntWritable(pb.getCount()));
                context.write(new TriplesDBKey(TriplesDBKey.STAR, TriplesDBKey.Y, dp.getY(), "NO_NATURAL"), new IntWritable(pb.getCount()));
                String pathStemmedK = dp.getPathStemmedK();
                if(testSet.containsKey(pathStemmedK)){
                    context.write(new TriplesDBKey(pathStemmedK, TriplesDBKey.X, TriplesDBKey.STAR, testSet.get(pathStemmedK)), new IntWritable(pb.getCount()));
                    context.write(new TriplesDBKey(pathStemmedK, TriplesDBKey.X, dp.getX(), testSet.get(pathStemmedK)), new IntWritable(pb.getCount()));
                    context.write(new TriplesDBKey(pathStemmedK, TriplesDBKey.Y, dp.getY(), testSet.get(pathStemmedK)), new IntWritable(pb.getCount()));
                }
            }
        }
    }
}
