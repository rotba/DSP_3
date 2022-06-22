package mr.joinwn;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class WNMapperTest {
    public static String testSet = "X respond to Y\tY eliminate X\n" +
            "X cure Y\tY relieve by X\n" +
            "X kill Y\tY die of X\n" +
            "X develop from Y\tX follow Y\n" +
            "X progress to Y\tX complicate by Y\n" +
            "X mean Y\tX cause Y\n" +
            "X overcome by Y\tX treat with Y\n" +
            "X accompany by Y\tY complicate X\n" +
            "X transmit by Y\tX spread through Y\n" +
            "X introduce by Y\tY produce X\n" +
            "X achieve with Y\tY indicate for X\n" +
            "X break Y\tX digest Y\n" +
            "X be during Y\tX occur during Y\n" +
            "X be with Y\tX characterize by Y\n" +
            "X penetrate by Y\tX fertilize by Y\n" +
            "X prevent Y\tY require X\n" +
            "X relieve by Y\tX control with Y\n" +
            "X treat by Y\tY alleviate X\n" +
            "X cure Y\tY control by X\n" +
            "X treat by Y\tY cure X\n" +
            "X help Y\tX alleviate Y\n" +
            "X develop Y\tX expose to Y\n" +
            "X infect with Y\tX transmit Y\n" +
            "X characterize by Y\tX cause by Y\n" +
            "X accompany Y\tY follow X\n" +
            "X attribute to Y\tX cause by Y";
    @Test
    public void testTestSetParsing() {
        Map<String,String> parsed = WNMapper.toHashMap(Utils.parseTestSet(testSet));

        for (String key:
                parsed.keySet()) {
            System.out.println(key);
            System.out.println(parsed.get(key));
        }
    }
}