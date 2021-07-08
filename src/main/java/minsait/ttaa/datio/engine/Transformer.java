package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;


import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();
        df.printSchema();
        df = columnSelection(df);
        write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shorName.column(),
                overall.column()
        );
    }

    private Dataset<Row> readInput() {
        return spark.read()
                .option(HEADER, true)
                .csv(INPUT_PATH);
    }

}
