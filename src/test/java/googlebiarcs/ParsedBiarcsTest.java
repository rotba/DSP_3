package googlebiarcs;

import mr.common.Utils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ParsedBiarcsTest {
    @Test
    public void testRawToPaths() {
        List<String> lines = readFile("/home/rotemb271/Code/School/bgu/extern/DSP_3/data/small/single_sent.txt");
        int counter = 0;
        for (String line:
             lines) {
            ParsedBiarcs pb = new ParsedBiarcs(line);
            counter++;
            if(!pb.headIsVerb()){
                continue;
            }
            for (DependecyPath dp:
                    pb.getDependencyPaths()) {
                if(dp.slotXTypeIsNoun() && dp.slotYTypeIsNoun())
                    System.out.println(dp.getPathStemmedK());
            }
        }
        System.out.println(counter);
    }

    @Test
    public void testStemmer() {
        List<String> lines = readFile("/home/rotemb271/Code/School/bgu/extern/DSP_3/data/positive-preds.txt");
        String all="";
        for (String line :
                lines) {
            all += line += "\n";
        }
        Map<String,String> map = Utils.toHashMap(all);
        for (String key:
             map.keySet()) {
            System.out.println(key);
            System.out.println(map.get(key));
        }
    }

    private static List<String> readFile(String path){
        List<String> ans = new ArrayList<>();
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                ans.add(data);
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            throw new RuntimeException(e);
        }
        return ans;
    }
}