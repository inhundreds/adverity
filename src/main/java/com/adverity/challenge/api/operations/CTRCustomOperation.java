package com.adverity.challenge.api.operations;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.col;

@AllArgsConstructor
public class CTRCustomOperation implements AbstractOperation {

    private RelationalGroupedDataset rgd;

    @Override
    public Dataset<Row> apply() {
        var opMap = Map.of(
            "clicks", "sum",
            "impressions", "sum"
        );
        Dataset<Row> dataset = rgd.agg(opMap);
        dataset = dataset.withColumn("ctr", col("sum(clicks)").multiply(100).divide(col("sum(impressions)")));
        dataset = dataset.drop("sum(clicks)").drop("sum(impressions)");
        return dataset;
    }

}
