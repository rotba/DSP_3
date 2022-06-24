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
    public DoubleWritable value;

    public SimValue() {
    }

    public SimValue(TriplesDBKey key, DoubleWritable value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value.readFields(dataInput);
    }

    public TriplesDBKey getKey() {
        return key;
    }

    public void setKey(TriplesDBKey key) {
        this.key = key;
    }

    public DoubleWritable getValue() {
        return value;
    }

    public void setValue(DoubleWritable value) {
        this.value = value;
    }
}
