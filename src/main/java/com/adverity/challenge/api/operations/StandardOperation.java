package com.adverity.challenge.api.operations;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@AllArgsConstructor
public class StandardOperation implements AbstractOperation {

    private RelationalGroupedDataset rgd;
    private Map<String, String> operationsMap;

    @Override
    public Dataset<Row> apply() {
        return rgd.agg(operationsMap);
    }

}
