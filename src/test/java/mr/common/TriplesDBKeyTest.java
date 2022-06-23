package mr.common;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

import static org.junit.Assert.*;

public class TriplesDBKeyTest {
    @Test
    public void testA() {
        List<String> lines = readFile("/home/rotemb271/Code/School/bgu/extern/DSP_3/other/keys_example.txt");
        List<TriplesDBKey> keys = new ArrayList<>();
        for (String line:
             lines) {
            try {
                TriplesDBKey parse = TriplesDBKey.parse(line);
                parse.setJoinPn(true);
                keys.add(parse);
            }catch (RuntimeException e){
                System.out.println(e.getMessage());
            }
        }
        keys.sort(new Comparator<TriplesDBKey>() {
            @Override
            public int compare(TriplesDBKey o1, TriplesDBKey o2) {
                return o1.compareTo(o2);
            }
        });
        for (TriplesDBKey k :
                keys) {
            System.out.println(k.toString());
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