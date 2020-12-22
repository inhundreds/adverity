package com.adverity.challenge.api.utils;

import com.adverity.challenge.api.controller.WebQuery;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryValidator {

    private static final Set<String> validMetrics = Set.of("clicks", "impressions", "ctr");
    private static final Set<String> aggregationOperation = Set.of("sum", "avg", "count", "min", "max");
    private static final Set<String> otherOperations = Set.of("");
    private static final Set<String> validOperations;

    static {
        validOperations = Stream.of(aggregationOperation, otherOperations).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public static boolean isValid(WebQuery wq)  {
        if (queryIsNotValid(wq)) return false;
        if (aFilterIsNotValid(wq)) return false;
        if (groupByIsNotValid(wq)) return false;
        return true;
    }

    private static boolean queryIsNotValid(WebQuery wq) {
        var metric = wq.getQuery().getMetric();
        var operation = wq.getQuery().getOperation();

        if (metric == null) return true;
        if (!validMetrics.contains(metric)) return true;
        if (!validOperations.contains(operation)) return true;
        return false;
    }

    private static boolean aFilterIsNotValid(WebQuery wq) {
        var optionalDateTo = wq.getFilters().getDateTo();
        var optionalDateFrom = wq.getFilters().getDateFrom();

        if (optionalDateTo.isPresent() && optionalDateFrom.isPresent()) {
            if (optionalDateTo.get().isBefore(optionalDateFrom.get())) return true;
        }
        return false;
    }

    private static boolean groupByIsNotValid(WebQuery wq) {
        var metric = wq.getQuery().getMetric();
        var operation = wq.getQuery().getOperation();
        var groupBy = wq.getGroupBy();

        if (groupBy.size()>0) {
            if (!metric.equals("ctr") && !aggregationOperation.contains(operation)) return true;
        }
        return false;
    }
}
