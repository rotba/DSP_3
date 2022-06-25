package mr.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.tartarus.snowball.ext.PorterStemmer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static String[][] toArray(String testset) {
        String[] arrOfStr = testset.split("\n");
        String[][] ans = new String[arrOfStr.length][];
        for (int i = 0; i < arrOfStr.length; i++) {
            ans[i] = arrOfStr[i].replace("\r","").split("\\t");
        }
        return ans;
    }

    public static Map<String, String> toHashMap(String testSetStr) {
        String[][] testSetRaw = toArray(testSetStr);
        Map<String,String> ans = new HashMap<>();
        for (int i = 0; i < testSetRaw.length; i++) {
            for (int j = 0; j < 2; j++) {
                String stemmedSentence = null;
                String[] words = testSetRaw[i][j].split("\\s+");
                words[0] = TriplesDBKey.X;
                words[words.length-1] = TriplesDBKey.Y;
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


    public static String[] addInputs(String percentage, String[] inputs) {
        double perc = ((double)Integer.parseInt(percentage)) / 100.0;
        int numOfInputs = (int)(inputs.length*perc);
        String[] ans = new String[numOfInputs];
        for (int i = 0; i < numOfInputs; i++) {
            ans[i] = inputs[i];
        }
        return ans;
    }
}
