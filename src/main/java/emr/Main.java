package emr;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class Main {

    public static void main(String[] args) {
        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.US_EAST_1)
                .build();
        String cid = args[0];
        String bucket= "s3://dsp3/";
        String baIn =  bucket+"ins/smalls/";
        String percentage = "100";
        String outBase = bucket+"out/";
        String posPreds = bucket+"tset/positive-preds.txt";
        String negPreds = bucket+"tset/negative-preds.txt";
        String large = "MILIONS";
        String slots = outBase+"slots";
        String wnOut = outBase+"wn"+cid;
        String miOut = outBase+"mi"+cid;
        String simOut = outBase+"sim"+cid;


        // Run a custom jar file as a step
        HadoopJarStepConfig wnConf = new HadoopJarStepConfig()
                .withJar(bucket+"jars/DSP_3-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("WN","NO_LOCAL", large,posPreds, negPreds,percentage, wnOut, slots); // optional list of arguments to pass to the jar
        StepConfig wnJarStep = new StepConfig("wn", wnConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig miConf = new HadoopJarStepConfig()
                .withJar(bucket+"jars/DSP_3-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("MI","NO_LOCAL",wnOut, slots, miOut); // optional list of arguments to pass to the jar
        StepConfig miJarStep = new StepConfig("mi", miConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig simConf = new HadoopJarStepConfig()
                .withJar(bucket+"jars/DSP_3-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("SIM","NO_LOCAL",posPreds, negPreds, miOut, simOut); // optional list of arguments to pass to the jar
        StepConfig simJarStep = new StepConfig("sim", simConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);


        AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(args[0]) // replace with cluster id to run the steps
                .withSteps(wnJarStep, miJarStep,simJarStep));

        System.out.println(result.getStepIds());

    }
}