package mr.common;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TDBRecordReader extends RecordReader<TriplesDBKey, TriplesDBValue> {

    private static final Logger logger = Logger.getLogger(TDBRecordReader.class);
    public static boolean LOCAL = false;
    public enum TDBRecordReaderCounters{
        PARSING_EXCEPTION
    }
    protected LineRecordReader reader;
    protected TriplesDBKey key;
    protected TriplesDBValue value;

    public TDBRecordReader() {
        reader = new LineRecordReader();
        key = null;
        value = null;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(reader.nextKeyValue()){
            try {
                key = TriplesDBKey.parse(reader.getCurrentValue().toString());
                String valString = reader.getCurrentValue().toString().substring(key.toString().length()+1);
                value = TriplesDBValue.parse(valString);
                if(LOCAL) logger.info(String.format("key:%s\tval:%s", key.toString(), valString.toString()));
                return true;
            }catch (RuntimeException e){
                throw e;
            }
        }else{
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public TriplesDBKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public TriplesDBValue getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
