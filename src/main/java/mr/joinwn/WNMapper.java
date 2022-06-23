package mr.joinwn;

import mr.calcmi.MiReducer;
import mr.common.TriplesDBKey;
import googlebiarcs.DependecyPath;
import googlebiarcs.ParsedBiarcs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.tartarus.snowball.ext.PorterStemmer;

import java.io.IOException;
import java.util.*;

public class WNMapper extends Mapper<Object, Text, TriplesDBKey, IntWritable> {
    Map<String,String> testSet = new HashMap<>();
    @Override
    protected void setup(Mapper<Object, Text, TriplesDBKey, IntWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        String[][] testSetRaw = Utils.parseTestSet(context.getConfiguration().get("TESTSET"));
        testSet = toHashMap(testSetRaw);
    }

    public static Map<String, String> toHashMap(String[][] testSetRaw) {
        Map<String,String> ans = new HashMap<>();
        for (int i = 0; i < testSetRaw.length; i++) {
            for (int j = 0; j < 2; j++) {
                String stemmedSentence = null;
                String[] words = testSetRaw[i][j].split("\\s+");
                for (int k = 0; k < words.length; k++) {
                    PorterStemmer porterStemmer = new PorterStemmer();
                    porterStemmer.setCurrent(words[k]);
                    porterStemmer.stem();
                    String stemmedWord = porterStemmer.getCurrent();
                    if(stemmedSentence == null)
                        stemmedSentence = stemmedWord;
                    else
                        stemmedSentence+=" "+stemmedWord;
                }
                ans.put(stemmedSentence, testSetRaw[i][j]);
            }
        }
        return ans;
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, TriplesDBKey, IntWritable>.Context context) throws IOException, InterruptedException {
        ParsedBiarcs pb = new ParsedBiarcs(value.toString());
        if(!pb.headIsVerb()){
            return;
        }
        List<DependecyPath> dps = pb.getDependencyPaths();
        for(DependecyPath dp: dps){
            if(dp.slotXTypeIsNoun()&& dp.slotYTypeIsNoun()){
                context.write(new TriplesDBKey(TriplesDBKey.STAR, TriplesDBKey.X, TriplesDBKey.STAR, ""), new IntWritable(pb.getCount()));
                context.write(new TriplesDBKey(TriplesDBKey.STAR, TriplesDBKey.X, dp.getX(), ""), new IntWritable(pb.getCount()));
                context.write(new TriplesDBKey(TriplesDBKey.STAR, TriplesDBKey.Y, dp.getY(), ""), new IntWritable(pb.getCount()));
                String pathStemmedK = dp.getPathStemmedK();
                if(testSet.containsKey(pathStemmedK)){
                    context.write(new TriplesDBKey(pathStemmedK, TriplesDBKey.X, TriplesDBKey.STAR, testSet.get(pathStemmedK)), new IntWritable(pb.getCount()));
                    context.write(new TriplesDBKey(pathStemmedK, TriplesDBKey.X, dp.getX(), testSet.get(pathStemmedK)), new IntWritable(pb.getCount()));
                    context.write(new TriplesDBKey(pathStemmedK, TriplesDBKey.Y, dp.getY(), testSet.get(pathStemmedK)), new IntWritable(pb.getCount()));
                }
            }
        }
    }
}
