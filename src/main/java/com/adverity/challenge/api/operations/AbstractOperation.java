package com.adverity.challenge.api.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface AbstractOperation {
    Dataset<Row> apply();
}
