package mr.joinwn;

import common.TriplesDBKey;
import common.TriplesDBValue;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class Main {
    public static final Logger logger = Logger.getLogger(Main.class);
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
        job.setCombinerClass(WNCCombiner.class);
        job.setReducerClass(WNCReducer.class);
        job.setPartitionerClass(WNPartitioner.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(TriplesDBKey.class);
        job.setOutputValueClass(TriplesDBValue.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(local.equals("LOCAL")) job.setNumReduceTasks(3);
        TextInputFormat.addInputPath(job, new Path(input));
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
        br.write("SLOTY\t"+job.getCounters().findCounter(WNCReducer.ReducerCounters.YSLOT_TOTAL).getValue()+"\n");
        br.close();
        fileSystem.close();
        System.exit(0 );
    }
}
