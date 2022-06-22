package common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TriplesDBValue implements Writable {
    private int slotN;
    private int pN;
    private int wN;
    private int n;

    public int getpN() {
        return pN;
    }

    public void setpN(int pN) {
        this.pN = pN;
    }

    public int getwN() {
        return wN;
    }

    public void setwN(int wN) {
        this.wN = wN;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public int getSlotN() {
        return slotN;
    }

    public void setSlotN(int slotN) {
        this.slotN = slotN;
    }

    public TriplesDBValue(int slotN, int pN, int wN, int n) {

        this.slotN = slotN;
        this.pN = pN;
        this.wN = wN;
        this.n = n;
    }

    public TriplesDBValue() {
    }

    @Override
    public String toString() {
        return slotN+"\t"+pN+"\t"+wN+"\t"+n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(slotN);
        dataOutput.writeInt(pN);
        dataOutput.writeInt(wN);
        dataOutput.writeInt(n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        slotN = dataInput.readInt();
        pN = dataInput.readInt();
        wN = dataInput.readInt();
        n = dataInput.readInt();
    }
}
