package mr.common;

import org.tartarus.snowball.ext.PorterStemmer;

import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static String[][] toArray(String testset) {
        String[] arrOfStr = testset.split("\n");
        String[][] ans = new String[arrOfStr.length][];
        for (int i = 0; i < arrOfStr.length; i++) {
            ans[i] = arrOfStr[i].split("\\t");
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
}
