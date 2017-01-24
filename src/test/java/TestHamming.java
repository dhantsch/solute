import org.apache.avro.generic.GenericData;
import solute.hive.udf.HammingDistance;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hah on 24.01.17.
 */
public class TestHamming {
    public static void main(String[] args) {
        String[] values = {"aaa", "abb", "aab",  "aac", "bbb"};
        int[] x = HammingDistance.minHammingDistance(values);
        System.out.println(x.length);
    }
}
