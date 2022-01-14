package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.CommonProperties;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;
    private CommonProperties properties;
    public Dataset<Row> getDf() {
        return df;
    }

    private Dataset<Row> df;
    public Transformer(@NotNull SparkSession spark) throws IOException {
        properties = new CommonProperties();

        this.spark = spark;
        df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = ageRange(df);
        df = rankNationalityPosition(df);
        df = potentialVSOverall(df);
        df = columnSelection(df);
        df = filterResults(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df , properties.getProperties().get("output_folder"));
    }

    /**
     *
     * @param df
     * @return
     */
    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                rankByNationalityPosition.column(),
                potentialOverall.column()
                // catHeightByPosition.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(properties.getProperties().get("input_data"));
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     *
     * @param df
     * @return
     */
    private Dataset<Row> ageRange(Dataset<Row> df){

        df = df.withColumn(ageRange.getName(),  when( age.column().$less(23), "A"  )
                                                .when(age.column().between(23,27), "B")
                                                .when(age.column().between(27,32), "C")
                                                .otherwise("D"));

        return df;
    }

    /**
     *
     * @param df
     * @return
     */
    private Dataset<Row> rankNationalityPosition(Dataset<Row> df) {

        WindowSpec w = Window
                .partitionBy(nationality.column() ,teamPosition.column() )
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        df = df.withColumn(rankByNationalityPosition.getName(), rank);

        return  df;
    }

    /**
     *
     * @param df
     * @return
     */
    private Dataset<Row> potentialVSOverall(Dataset<Row> df) {
        return df.withColumn(potentialOverall.getName(), round(potential.column().divide(overall.column()), 4)  );
    }

    private Dataset<Row> filterResults(Dataset<Row> df){
        df = df.filter(rankByNationalityPosition.column().$less(3)
                .or( ageRange.column().isin("B", "C").and(potentialOverall.column().$greater(1.15) ) )
                .or( ageRange.column().equalTo("A").and(potentialOverall.column().$greater(1.25) ) )
                .or( ageRange.column().equalTo("D").and(rankByNationalityPosition.column().$less(5) ) )
        );
        return df;
    }
}
