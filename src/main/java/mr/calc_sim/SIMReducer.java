package mr.calc_sim;

import mr.common.TriplesDBKey;
import mr.joinwn.WNCReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SIMReducer extends Reducer<Text, SimValue, Text, DoubleWritable> {
    static final boolean LOCAL = false;
    private static final Logger logger = Logger.getLogger(SIMReducer.class);

    public enum SIMReducerCounters {
        KEY_VAL_NOT_MATCH
    }

    @Override
    protected void reduce(Text key, Iterable<SimValue> values, Reducer<Text, SimValue, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        List<SimValue> p1XFeatures = new ArrayList<>();
        List<SimValue> p2XFeatures = new ArrayList<>();
        List<SimValue> p1YFeatures = new ArrayList<>();
        List<SimValue> p2YFeatures = new ArrayList<>();
        String[] sentences = key.toString().split("\t");
        for (SimValue val :
                values) {
            if (LOCAL)
                logger.info(String.format("key:%s, val.k:%s, val.val:%s", key.toString(), val.key.toString(), Double.toString(val.getValue())));
            TriplesDBKey valKey = val.key;
            if (valKey.getSlot().equals(TriplesDBKey.X) && valKey.getNaturalK().equals(sentences[0])) {
                p1XFeatures.add(val);
            } else if (valKey.getSlot().equals(TriplesDBKey.X) && valKey.getNaturalK().equals(sentences[1])) {
                p2XFeatures.add(val);
            } else if (valKey.getSlot().equals(TriplesDBKey.Y) && valKey.getNaturalK().equals(sentences[0])) {
                p1YFeatures.add(val);
            } else if (valKey.getSlot().equals(TriplesDBKey.Y) && valKey.getNaturalK().equals(sentences[1])) {
                p2YFeatures.add(val);
            } else {
                context.getCounter(SIMReducerCounters.KEY_VAL_NOT_MATCH).increment(1);
                return;
            }
        }
        String[] s2 = sentences[1].split(" ");
        double simX;
        double simY;
        if (LOCAL) logger.info(String.format("Calc sim for p1:%s, p2:%s", sentences[0], sentences[1]));
        if (s2[0].equals(TriplesDBKey.X)) {
            simX = calcSim(p1XFeatures, p2XFeatures);
            simY = calcSim(p1YFeatures, p2YFeatures);
        } else {
            simX = calcSim(p1XFeatures, p2YFeatures);
            simY = calcSim(p2XFeatures, p1YFeatures);
        }
        context.write(new Text(sentences[0] + " " + sentences[1]), new DoubleWritable(Math.sqrt(simX * simY)));
    }

    private double calcSim(List<SimValue> p1Features, List<SimValue> p2Features) {
        double num = 0.0;
        double den = 0.0;
        boolean summedP2Sum = false;
        for (SimValue p1F :
                p1Features) {
            for (SimValue p2F :
                    p2Features) {
                if (p1F.getKey().getW().equals(p2F.getKey().getW())) {
                    num += p1F.getValue() + p2F.getValue();
                    if (LOCAL)
                        logger.info(String.format("T: w:%s, p1.mi:%s, p2.mi:%s -> num:%s", p1F.getKey().getW(), Double.toString(p1F.getValue()), Double.toString(p2F.getValue()), Double.toString(num)));
                }
                if (!summedP2Sum) {
                    den += p2F.getValue();
                    if (LOCAL)
                        logger.info(String.format("ALL: w:%s, p2.mi:%s, den:%s", p2F.getKey().getW(), Double.toString(p2F.getValue()), Double.toString(den)));
                }
            }
            summedP2Sum = true;
            den += p1F.getValue();
            if (LOCAL)
                logger.info(String.format("ALL: w:%s, p1.mi:%s, den:%s", p1F.getKey().getW(), Double.toString(p1F.getValue()), Double.toString(den)));
        }
        if (den == 0) {
            return 0;
        }
        return num / den;
    }
}
