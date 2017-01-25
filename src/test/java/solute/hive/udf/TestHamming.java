package solute.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHamming {

    @Test
    public void testMinDistance() {
        String[] values = {"aaa", "abb", "aab",  "aac", "bbb"};
        int minDistance = HammingDistance.minHammingDistance(values);

        assertEquals(1, minDistance);
    }

    @Test
    public void testByteDistance() {
        byte[] valB1 = {5, 7};
        byte[] valB2 = {6, 8};

        assertEquals(6, HammingDistance.hammingDistance(valB1, valB2));
    }

    @Test
    public void testHamminDistanceUTF() throws HiveException {

        // set up the models we need
        MinHammingDistanceUDF udf = new MinHammingDistanceUDF();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);

        JavaIntObjectInspector resultInspector = (JavaIntObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        // create the actual UDF arguments
        List<String> list = new ArrayList<>();
        list.add("aaa");
        list.add("abb");
        list.add("aab");
        list.add("aac");
        list.add("bbb");

        // the value exists
        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(list)});
        assertEquals(1, resultInspector.get(result));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEmptyDistanceUTF() throws HiveException {

        // set up the models we need
        MinHammingDistanceUDF udf = new MinHammingDistanceUDF();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);

        JavaIntObjectInspector resultInspector = (JavaIntObjectInspector) udf.initialize(new ObjectInspector[]{listOI});

        // create the actual UDF arguments
        List<String> list = new ArrayList<>();

        // the value exists
        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(list)});
    }

}
