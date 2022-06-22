package common;

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
    }

    public TriplesDBKey(String stemmedK, String slot, String w, String naturalK) {
        this.stemmedK = stemmedK;
        this.slot = slot;
        this.w = w;
        this.naturalK = naturalK;
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
        if(stemmedK.equals(o.stemmedK) && slot.equals(o.slot)){
            return 0;
        }
        if(w.equals(STAR)){
            if(o.w.equals(STAR)){
                return stemmedK.compareTo(o.stemmedK);
            }
            return -1;
        }
        if(stemmedK.equals(STAR)){
            if(o.w.equals(w)){
                return -1;
            }else{
                return w.compareTo(o.w);
            }
        }
        if(o.w.equals(STAR)){
            return 1;
        }else if(o.stemmedK.equals(STAR) && w.equals(o.w)){
            return 1;
        }else{
            return w.compareTo(o.w);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(stemmedK);
        dataOutput.writeUTF(slot);
        dataOutput.writeUTF(w);
        dataOutput.writeUTF(naturalK);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stemmedK = dataInput.readUTF();
        slot = dataInput.readUTF();
        w = dataInput.readUTF();
        naturalK = dataInput.readUTF();
    }
}
