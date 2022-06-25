package mr.calcmi;

import mr.common.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;

public class Main {
    public static final Logger logger = Logger.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        int argsIndex =0;
        String local = args[argsIndex++];
        String input = args[argsIndex++];
        String slotInput =args[argsIndex++];
        String output = args[argsIndex++];
        Configuration conf = new Configuration();
        Path pathSlot = new Path(slotInput);
        FileSystem fs = pathSlot.getFileSystem(conf);
        String slotsStr;
        try{
            FSDataInputStream inputStream = fs.open(pathSlot);
            slotsStr = IOUtils.toString(inputStream, "UTF-8");
            fs.close();
        }catch (FileNotFoundException e){
            throw e;
        }finally{
            fs.close();
        }
        if (local.equals("LOCAL")){
            conf.set("SLOTS", "SLOTX\t1000");
        }else{
            conf.set("SLOTS", slotsStr);
        }
        conf.reloadConfiguration();
        Job job = Job.getInstance(conf, "joinwn");
        job.setJarByClass(Main.class);
        job.setMapperClass(MiMapper.class);
        job.setReducerClass(MiReducer.class);
        job.setPartitionerClass(TBDKeyPartitioner.class);
        job.setMapOutputValueClass(TriplesDBValue.class);
        job.setOutputKeyClass(TriplesDBKey.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TDBKInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(local.equals("LOCAL")) job.setNumReduceTasks(3);
        conf.set("LOCAL", local);
        conf.reloadConfiguration();
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
