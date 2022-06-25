package mr.joinwn;

import mr.common.TBDKeyCombiner;
import mr.common.TBDKeyPartitioner;
import mr.common.TriplesDBKey;
import mr.common.TriplesDBValue;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;

public class Main {
    public static final Logger logger = Logger.getLogger(Main.class);
    public static String[] localInputs = {
            "/2test/in/small/"
    };
    public static String[] remoteInputs = {
            "s3://dsp3/ins/milions/biarcs.10-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.20-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.30-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.40-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.50-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.60-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.70-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.80-of-99.gz",
            "s3://dsp3/ins/milions/biarcs.90-of-99.gz",
    };
    public static void main(String[] args) throws Exception {
        int argsIndex =0;
        String local = args[argsIndex++];
        String testSetPathPositive = args[argsIndex++];
        String testSetPathNegative = args[argsIndex++];
        String input = args[argsIndex++];
        String output = args[argsIndex++];
        String slotOutPut =args[argsIndex];
        Configuration conf = new Configuration();
        String testSet = "";
        Path pathPos = new Path(testSetPathPositive);
        Path pathNeg = new Path(testSetPathNegative);
        FileSystem fs = pathPos.getFileSystem(conf);
        FileSystem fsNeg = pathNeg.getFileSystem(conf);
        try{
            FSDataInputStream inputStreamPositive = fs.open(pathPos);
            FSDataInputStream inputStreamNegative = fsNeg.open(pathNeg);
            String chunkPos = IOUtils.toString(inputStreamPositive, "UTF-8");
            String chunkNeg = IOUtils.toString(inputStreamNegative, "UTF-8");
            testSet= chunkPos+chunkNeg;
            fs.close();
        }catch (FileNotFoundException e){
            throw e;
        }finally{
            fs.close();
        }
        conf.set("TESTSET", testSet);
        conf.reloadConfiguration();
        Job job = Job.getInstance(conf, "joinwn");
        job.setJarByClass(Main.class);
        job.setMapperClass(WNMapper.class);
        job.setCombinerClass(TBDKeyCombiner.class);
        job.setReducerClass(WNCReducer.class);
        job.setPartitionerClass(TBDKeyPartitioner.class);
        job.setMapOutputKeyClass(TriplesDBKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        if(local.equals("LOCAL")) job.setNumReduceTasks(3);
        String[] inputs = mr.common.Utils.addInputs(input, local.equals("LOCAL") ? localInputs: remoteInputs);
        for (int i = 0; i < inputs.length; i++) {
            TextInputFormat.addInputPath(job, new Path(inputs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(output));
        boolean b = job.waitForCompletion(true);
        if(!b){
            System.exit(1);
        }
        Path file = new Path(slotOutPut);
        FileSystem fileSystem = file.getFileSystem(conf);
        if ( fileSystem.exists( file )) { fileSystem.delete( file, true ); }
        OutputStream os = fileSystem.create(file);
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write("SLOTX\t"+job.getCounters().findCounter(WNCReducer.ReducerCounters.XSLOT_TOTAL).getValue()+"\n");
        br.close();
        fileSystem.close();
        System.exit(0 );
    }

}
