package com.adverity.challenge.api.operations;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

@AllArgsConstructor
public class NoOperation implements AbstractOperation {

    private RelationalGroupedDataset rgd;

    @Override
    public Dataset<Row> apply() {
        return rgd.df();
    }

}
