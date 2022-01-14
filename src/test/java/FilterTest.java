import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import static org.apache.spark.sql.functions.not;
import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.Common.INPUT_PATH;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;

public class FilterTest{

    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    @Test
    public void filterTest(){
        Transformer t = new Transformer(spark);
        Dataset df = t.getDf();

        df = df.filter( not(rankByNationalityPosition.column().$less(3))
                .and( not(ageRange.column().isin("B","C")).or(not(potentialOverall.column().$greater(1.15))) )
                .and( not(ageRange.column().equalTo("A")).or(not(potentialOverall.column().$greater(1.25))) )
                .and( not(ageRange.column().equalTo("D")).or(not(rankByNationalityPosition.column().$less(5))))
        );
        assert ( df.count() == 0 );
    }
}
