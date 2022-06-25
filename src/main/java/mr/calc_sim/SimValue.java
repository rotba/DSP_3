package mr.calc_sim;

import mr.common.TriplesDBKey;
import mr.common.TriplesDBValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SimValue implements Writable {
    public TriplesDBKey key;
    public double value;

    public SimValue() {
        key = new TriplesDBKey();
    }

    public SimValue(TriplesDBKey key, double value) {
        this.key = key;
        this.value = value;
    }

    public SimValue(SimValue val) {
        this.key = new TriplesDBKey(val.key);
        this.value = val.value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value = dataInput.readDouble();
    }

    public TriplesDBKey getKey() {
        return key;
    }

    public void setKey(TriplesDBKey key) {
        this.key = key;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
