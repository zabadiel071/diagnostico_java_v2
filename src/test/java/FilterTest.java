import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.spark.sql.functions.not;
import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;

public class FilterTest{

    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    /**
     * Test filtered players are correct by filter oposite conditions
     * Expecting result to be 0 rows retrieved
     */
    @Test
    public void filterTest(){
        Transformer t = null;
        try {
            t = new Transformer(spark);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Dataset df = t.getDf();

        df = df.filter( not(rankByNationalityPosition.column().$less(3))
                .and( not(ageRange.column().isin("B","C")).or(not(potentialOverall.column().$greater(1.15))) )
                .and( not(ageRange.column().equalTo("A")).or(not(potentialOverall.column().$greater(1.25))) )
                .and( not(ageRange.column().equalTo("D")).or(not(rankByNationalityPosition.column().$less(5))))
        );
        Assert.assertEquals(0,df.count());
    }
}
