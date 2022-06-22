package mr.joinwn;

import java.util.Set;

public class Utils {
    public static String[][] parseTestSet(String testset) {
        String[] arrOfStr = testset.split("\n");
        String[][] ans = new String[arrOfStr.length][];
        for (int i = 0; i < arrOfStr.length; i++) {
            ans[i] = arrOfStr[i].split("\\t");
        }
        return ans;
    }
}
