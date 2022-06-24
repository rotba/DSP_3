package mr.calc_sim;

import mr.common.TriplesDBKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SIMReducer extends Reducer<Text, SimValue, Text, DoubleWritable> {
    public enum SIMReducerCounters{
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
            TriplesDBKey valKey = val.key;
            if (valKey.getSlot().equals(TriplesDBKey.X) && valKey.getNaturalK().equals(sentences[0])){
                p1XFeatures.add(val);
            } else if (valKey.getSlot().equals(TriplesDBKey.X) && valKey.getNaturalK().equals(sentences[1])) {
                p2XFeatures.add(val);
            } else if (valKey.getSlot().equals(TriplesDBKey.Y) && valKey.getNaturalK().equals(sentences[0])) {
                p1YFeatures.add(val);
            } else if (valKey.getSlot().equals(TriplesDBKey.Y) && valKey.getNaturalK().equals(sentences[1])) {
                p2YFeatures.add(val);
            }else{
                context.getCounter(SIMReducerCounters.KEY_VAL_NOT_MATCH).increment(1);
                return;
            }
        }
        double simX = calcSim(p1XFeatures, p2XFeatures);
        double simY = calcSim(p1YFeatures, p2YFeatures);
        context.write(new Text(sentences[0]+" "+sentences[1]), new DoubleWritable(Math.sqrt(simX*simY)));
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
                    num+=p1F.getValue().get()+p2F.getValue().get();
                }
                if(!summedP2Sum){
                    den+=p2F.getValue().get();
                }
            }
            summedP2Sum = true;
            den+=p1F.getValue().get();
        }
        return num/den;
    }
}
