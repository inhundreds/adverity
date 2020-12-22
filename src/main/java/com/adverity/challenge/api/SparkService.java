package com.adverity.challenge.api;

import com.adverity.challenge.api.controller.WebQuery;
import com.adverity.challenge.api.operations.OperationFactory;
import com.adverity.challenge.api.results.Table;

import com.adverity.challenge.api.utils.QueryValidator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class SparkService {

    public static final String INVALID_QUERY = "There is an error in the query";
    private static final Table ERROR_RESULT = new Table(List.of(List.of(INVALID_QUERY)), List.of());

    @Autowired
    private final SparkAdapter sparkAdapter;

    public SparkService(SparkAdapter sparkAdapter) {
        this.sparkAdapter = sparkAdapter;
    }

    public Table queryDataset(WebQuery wq)  {
        if (!QueryValidator.isValid(wq)) return ERROR_RESULT;
        Dataset<Row> df = getResultDataset(wq);
        return getResultAsListOfString(df);
    }

    private Dataset<Row> getResultDataset(WebQuery wq) {
        Dataset<Row> df = sparkAdapter.getDataset();
        df = applyFilters(df, wq);
        var rgd = applyGroupBy(df, wq);
        df = applyOperation(wq, rgd);
        df.show();
        return df;
    }

    private Dataset<Row> applyFilters(Dataset<Row> df, WebQuery wq) {
        String[] filters = getFilters(wq.getFilters());
        for(var currentFilter : filters) {
            df = df.filter(currentFilter);
        }
        return df;
    }

    private RelationalGroupedDataset applyGroupBy(Dataset<Row> df, WebQuery wq) {
        Column[] colsGroups = getColumnsForGroupBy(wq.getGroupBy());
        return df.groupBy(colsGroups);
    }

    private Dataset<Row> applyOperation(WebQuery wq, RelationalGroupedDataset rgd) {
        Dataset<Row> df;
        var query = wq.getQuery();
        var metric = query.getMetric();
        var operation = query.getOperation();
        df = OperationFactory.createOperation(metric, operation, rgd).apply();
        return df;
    }

    private Table getResultAsListOfString(Dataset<Row> df) {
        var columns = List.of(df.columns());
        return new Table(df.collectAsList().stream().map(
                row -> List.of(row.mkString(",").split(","))
        ).collect(Collectors.toList()), columns);
    }

    private String[] getFilters(WebQuery.Filters filters) {
        List<String> filtersList = new ArrayList<>();

        filters.getDatasource().ifPresent(
                datasource -> filtersList.add(String.format("datasource == '%s'", datasource))
        );

        filters.getCampaign().ifPresent(
                campaign -> filtersList.add(String.format("campaign == '%s'", campaign))
        );

        filters.getDateFrom().ifPresent(
                dateFrom -> filtersList.add(String.format("daily >= '%s'", dateFrom))
        );

        filters.getDateTo().ifPresent(
                dateTo -> filtersList.add(String.format("daily <= '%s'", dateTo))
        );

        return filtersList.toArray(new String[0]);
    }

    private Column[] getColumnsForGroupBy(List<WebQuery.Group> groupBy) {
        List<Column> cols = new ArrayList<>();
        groupBy.forEach(
            group -> cols.add(new Column(group.getFieldName()))
        );
        return cols.toArray(new Column[0]);
    }

}
