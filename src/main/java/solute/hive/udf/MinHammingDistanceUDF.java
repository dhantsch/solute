package solute.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import solute.hive.udf.HammingDistance;

import java.util.List;

/**
 * This UDF provides a row_number() function.
 */
@Description(name = "minHummingDistance", value = "_FUNC_(value, partition columns ...) - Returns the humming distance of a value within a partitioned, sorted window.")
@UDFType(deterministic = false, stateful = true)
public class MinHammingDistanceUDF extends GenericUDF {

    private ListObjectInspector listOI;

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<String> values = (List<String>) this.listOI.getList(arguments[0].get());
        return HammingDistance.minHammingDistance(values.toArray(new String[0]));
    }

    @Override
    public String getDisplayString(String[] currentKey) {
        return "solute.hive.udf.MinHammingDistanceUDF Distance";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 1 argument: List<T>");
        }

        // Check we received the right object types.
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof ListObjectInspector)) {
            throw new UDFArgumentException("the argument must be a list of strings");
        }

        this.listOI = (ListObjectInspector) arg;
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }
}