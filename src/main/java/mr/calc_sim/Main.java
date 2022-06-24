package mr.calc_sim;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
        String testSetPathPositive = args[argsIndex++];
        String testSetPathNegative = args[argsIndex++];
        String input = args[argsIndex++];
        String output = args[argsIndex++];
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
        Job job = Job.getInstance(conf, "calc_sim");
        job.setJarByClass(Main.class);
        job.setMapperClass(SIMMapper.class);
        job.setPartitionerClass(SIMPartitioner.class);
        job.setReducerClass(SIMReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SimValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SimInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        if(local.equals("LOCAL")) job.setNumReduceTasks(1);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
