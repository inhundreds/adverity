package com.adverity.challenge.api.operations;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.RelationalGroupedDataset;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OperationFactory {

    public static AbstractOperation createOperation(String metric, String operation, RelationalGroupedDataset rgd) {
        if (operation.equals("")) {
            if (metric.equals("ctr"))
                return new CTRCustomOperation(rgd);
            else
                return new NoOperation(rgd);
        }
        else
            return new StandardOperation(rgd, Map.of(metric, operation));
    }

}
