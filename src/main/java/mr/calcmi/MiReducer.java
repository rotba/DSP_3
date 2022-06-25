package mr.calcmi;

import mr.common.TriplesDBKey;
import mr.common.TriplesDBValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MiReducer extends Reducer<TriplesDBKey, TriplesDBValue, TriplesDBKey, DoubleWritable> {
    private static final Logger logger = Logger.getLogger(MiReducer.class);
    static final boolean LOCAL = false;
    public enum MiReducerCounters{
        NOT_SORTED,
        NON_SINGLE_VAL,
        WN_IS_MINUS_1,
        N_IS_MINUS_1,
        }
    int pN;
    int xN;
    String p;
    @Override
    protected void setup(Reducer<TriplesDBKey, TriplesDBValue, TriplesDBKey, DoubleWritable>.Context context) throws IOException, InterruptedException {

        super.setup(context);
        String slots = context.getConfiguration().get("SLOTS");
        String[] x = slots.split("\\s+");
        xN = Integer.parseInt(x[1]);
    }
    @Override
    protected void reduce(TriplesDBKey key, Iterable<TriplesDBValue> values, Reducer<TriplesDBKey, TriplesDBValue, TriplesDBKey, DoubleWritable>.Context context) throws IOException, InterruptedException {
        if(LOCAL) logger.info(String.format("key:%s", key.toString()));
        if(key.getW().equals(TriplesDBKey.STAR)){
            int sum = 0;
            for (TriplesDBValue val :
                    values) {
                sum += val.getpN();
            }
            pN = sum;
            p= key.getStemmedK();
        }else{
            if(p ==null || !p.equals(key.getStemmedK())){
                context.getCounter(MiReducerCounters.NOT_SORTED).increment(1);
                return;
            }
            TriplesDBValue theVal = null;
            for (TriplesDBValue val :
                    values) {
                if (LOCAL)logger.info(String.format("val:%s", val.toString()));
                if (theVal != null){
                    if (LOCAL)logger.info(String.format("non_sing_key:%s", key.toString()));
                    context.getCounter(MiReducerCounters.NON_SINGLE_VAL).increment(1);
                    return;
                }
                theVal =val;
            }
            int slot = xN;
            int wN = theVal.getwN();
            int n = theVal.getN();
            if(wN == -1){
                context.getCounter(MiReducerCounters.WN_IS_MINUS_1).increment(1);
                return;
            }
            if(n == -1){
                context.getCounter(MiReducerCounters.N_IS_MINUS_1).increment(1);
                return;
            }
            double res = calcMi(slot, pN, wN, n);
            if (LOCAL) logger.info(String.format("key:%s, slot:%d, pN:%d, wN:%d, n:%d,mi:%s", key.toString(), slot, pN, wN, n, Double.toString(res)));
            context.write(key, new DoubleWritable(res));
        }
    }

    private double calcMi(double slot, double pN, double wN, double n) {
        return Math.log((n*slot)/(pN*wN));
    }
}
