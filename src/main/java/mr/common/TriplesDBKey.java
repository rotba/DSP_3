package mr.common;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TriplesDBKey implements WritableComparable<TriplesDBKey> {
    public static final String STAR = "*";
    public static final String X = "X";
    public static final String Y = "Y";
    private String stemmedK;
    private String slot;
    private String w;
    private String naturalK;

    private boolean joinPn;

    public TriplesDBKey(TriplesDBKey key) {
        stemmedK = key.getStemmedK();
        slot = key.getSlot();
        w = key.getW();
        naturalK = key.getNaturalK();
    }

    public String getStemmedK() {
        return stemmedK;
    }

    public void setStemmedK(String stemmedK) {
        this.stemmedK = stemmedK;
    }

    @Override
    public String toString() {
        return stemmedK+"\t"+slot+"\t"+w+"\t"+naturalK;
    }

    public static TriplesDBKey parse(String line){
        try {
            String[] splitted = line.split("\\t");
            return new TriplesDBKey(splitted[0],splitted[1],splitted[2],splitted[3]);
        }catch (Exception e){
            throw new RuntimeException(String.format("line:%s", line));
        }
    }

    public String getSlot() {
        return slot;
    }

    public void setSlot(String slot) {
        this.slot = slot;
    }

    public String getW() {
        return w;
    }

    public void setW(String w) {
        this.w = w;
    }

    public String getNaturalK() {
        return naturalK;
    }

    public void setNaturalK(String naturalK) {
        this.naturalK = naturalK;
    }

    public TriplesDBKey() {
        joinPn = false;
    }

    public TriplesDBKey(String stemmedK, String slot, String w, String naturalK) {
        this.stemmedK = stemmedK;
        this.slot = slot;
        this.w = w;
        this.naturalK = naturalK;
        joinPn = false;
    }


    @Override
    public int compareTo(TriplesDBKey o) {
        if(stemmedK.equals(o.stemmedK) && slot.equals(o.slot) && w.equals(o.w)){
            return 0;
        }
        if(stemmedK.equals(STAR) && w.equals(STAR)){
            if(o.stemmedK.equals(STAR) && o.w.equals(STAR)&& o.slot.equals(X)){
                return 1;
            }else{
                return -1;
            }
        }
        if(stemmedK.equals(o.stemmedK) && w.equals(o.w)){
            return slot.equals(X) ? -1 : 1;
        }
        if(joinPn){
            return applyJoinPn(o);
        }else{
            return applyJoinWn(o);
        }
    }

    private int applyJoinPn(TriplesDBKey o) {
        if(w.equals(TriplesDBKey.STAR) && o.stemmedK.equals(stemmedK)){
            return -1;
        }else if(o.w.equals(TriplesDBKey.STAR) && o.stemmedK.equals(stemmedK)){
            return 1;
        } else if(stemmedK.equals(o.stemmedK)){
            return w.compareTo(o.w);
        }else{
            return stemmedK.compareTo(o.stemmedK);
        }
    }

    private int applyJoinWn(TriplesDBKey o) {
        if(w.equals(STAR) && o.w.equals(STAR)){
            return stemmedK.compareTo(o.stemmedK);
        }else if(w.equals(STAR)){
            return -1;
        }else if (o.w.equals(STAR)){
            return 1;
        }
        if(stemmedK.equals(STAR) && o.w.equals(w)){
            return -1;
        }else if(o.stemmedK.equals(STAR)&& o.w.equals(w)){
            return 1;
        }else if (!w.equals(o.w)){
            return w.compareTo(o.w);
        } else {
            return stemmedK.compareTo(o.stemmedK);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(stemmedK);
        dataOutput.writeUTF(slot);
        dataOutput.writeUTF(w);
        dataOutput.writeUTF(naturalK);
        dataOutput.writeBoolean(joinPn);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stemmedK = dataInput.readUTF();
        slot = dataInput.readUTF();
        w = dataInput.readUTF();
        naturalK = dataInput.readUTF();
        joinPn = dataInput.readBoolean();
    }

    public boolean isJoinPn() {
        return joinPn;
    }

    public void setJoinPn(boolean joinPn) {
        this.joinPn = joinPn;
    }
}
