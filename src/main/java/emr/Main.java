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
        String mode = args[1];
        String ogIn = null;
        String tgIn = null;
        String debug = "0";
        String cid = args[0];
        String perc = "100";

        String bigramThresh;
        if(mode.equals("TEST")){
            ogIn = "s3://jarbucket1653138772100/sin/1g/";
            tgIn = "s3://jarbucket1653138772100/sin/2g/";
            debug = "1";
            bigramThresh = "6";
        }else if(mode.equals("ENG")){
            bigramThresh = "20";
            ogIn = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
            tgIn = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
        }else if(mode.equals("HEB")){
            bigramThresh = "6";
            ogIn = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
            tgIn = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
        }else if(mode.equals("AB")) {
            bigramThresh = "20";
            ogIn = "s3://jarbucket1653138772100/ggl/fic/1g/";
            tgIn = "s3://jarbucket1653138772100/ggl/fic/2g/abfic.lzo";
        }else{
            throw new RuntimeException(String.format("Given mode - %s - not supported. ENG, HEB or TEST", mode));
        }

        // Run a custom jar file as a step
        HadoopJarStepConfig ogwcConf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("OGWC",debug,"NO_LOCAL",bigramThresh, perc,ogIn, "s3://jarbucket1653138772100/out/ogwc"+cid); // optional list of arguments to pass to the jar
        StepConfig ogwcJarStep = new StepConfig("ogwc", ogwcConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

//        HadoopJarStepConfig tgwcConf = new HadoopJarStepConfig()
//                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
//                .withArgs("TGWC",debug,"NO_LOCAL",bigramThresh, perc,tgIn, "s3://jarbucket1653138772100/out/tgwc"); // optional list of arguments to pass to the jar
//        StepConfig tgwcJarStep = new StepConfig("tgwc", tgwcConf)
//                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig tgwcCombConf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("TGWCComb",debug,"NO_LOCAL",bigramThresh, perc,tgIn, "s3://jarbucket1653138772100/out/tgwccomb"+cid); // optional list of arguments to pass to the jar
        StepConfig tgwcJarCombStep = new StepConfig("tgwccomb", tgwcCombConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig deccConf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("DECC",debug,"NO_LOCAL",bigramThresh, perc,ogIn, "s3://jarbucket1653138772100/out/decc"+cid); // optional list of arguments to pass to the jar
        StepConfig deccJarStep = new StepConfig("decc", deccConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig nc1Conf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("NC1AP",debug,"NO_LOCAL",bigramThresh, perc,"s3://jarbucket1653138772100/out/decc"+cid+"/part-r-00000", "s3://jarbucket1653138772100/out/tgwccomb"+cid, "s3://jarbucket1653138772100/out/ogwc"+cid, "s3://jarbucket1653138772100/out/nc1"+cid); // optional list of arguments to pass to the jar
        StepConfig nc1JarStep = new StepConfig("nc1ap", nc1Conf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig c2Conf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("C2AP",debug,"NO_LOCAL",bigramThresh, perc,"s3://jarbucket1653138772100/out/ogwc"+cid, "s3://jarbucket1653138772100/out/nc1"+cid, "s3://jarbucket1653138772100/out/nc2"+cid); // optional list of arguments to pass to the jar
        StepConfig c2JarStep = new StepConfig("nc2", c2Conf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig lklConf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("LKL",debug,"NO_LOCAL",bigramThresh, perc,"s3://jarbucket1653138772100/out/nc2"+cid,"s3://jarbucket1653138772100/fout"+cid); // optional list of arguments to pass to the jar
        StepConfig lklJarStep = new StepConfig("lkl", lklConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        HadoopJarStepConfig TMPlklConf = new HadoopJarStepConfig()
                .withJar("s3://jarbucket1653138772100/DSP_2-1.0-SNAPSHOT-jar-with-dependencies.jar") // replace with the location of the jar to run as a step
                .withArgs("LKL",debug,"NO_LOCAL",bigramThresh, perc,"s3://jarbucket1653138772100/out/nc2j-1DFJDUJD78PTU/","s3://jarbucket1653138772100/fouts/heb1/"); // optional list of arguments to pass to the jar
        StepConfig TMPlklJarStep = new StepConfig("lkl", TMPlklConf)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
                .withJobFlowId(args[0]) // replace with cluster id to run the steps
                .withSteps(ogwcJarStep, tgwcJarCombStep, deccJarStep, nc1JarStep, c2JarStep,lklJarStep));

        System.out.println(result.getStepIds());

    }
}