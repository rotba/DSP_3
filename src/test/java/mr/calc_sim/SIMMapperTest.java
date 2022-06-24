package mr.calc_sim;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.junit.Assert.*;

public class SIMMapperTest {
    @Test
    public void testReplace() {

        System.out.println(SIMMapper.replaceXY("X complete Y"));
        System.out.println(SIMMapper.replaceXY("Y complete X"));
    }
}