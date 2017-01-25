package solute.hive.udf;

import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class UserDefinedFunctionTest {


    private final String hdfsSource = "${hiveconf:hadoop.tmp.dir}/udf";

    @HiveSetupScript
    String setup =
            "  CREATE TABLE udf_test (" +
                    " id int," +
                    " value string" +
                    "  )" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '" + hdfsSource + "' ;";

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell hiveShell;

    @Test
    public void udfMax() {
        hiveShell.addResource(hdfsSource + "/data.csv","123\tv1\n124\tv2\n125\tv3");
        hiveShell.start();
        Assert.assertEquals(Arrays.asList("125"), hiveShell.executeQuery("SELECT max(id) FROM udf_test"));
    }

    @Test
    public void udfMin() {
        hiveShell.addResource(hdfsSource + "/data.csv","123\tv1\n124\tv2\n125\tv3");
        hiveShell.start();
        Assert.assertEquals(Arrays.asList("123"), hiveShell.executeQuery("SELECT min(id) FROM udf_test"));
    }

    @Test
    public void regexp_extract() {
        hiveShell.addResource(hdfsSource + "/data.csv","1\t123ABC");
        hiveShell.start();
        List<String> expected = Arrays.asList("123");
        List<String> actual = hiveShell.executeQuery("SELECT regexp_extract(value, '([0-9]*)[A-Z]*', 1) FROM udf_test");
        Assert.assertEquals(expected, actual);
    }

    @Test
    @Ignore
    public void hammingDistance() {
        hiveShell.addResource(hdfsSource + "/data.csv","1\t123ABC");
        hiveShell.addSetupScript("CREATE FUNCTION hdist as 'solute.hive.udf.MinHammingDistanceUDF'");
        hiveShell.start();
    }
}
