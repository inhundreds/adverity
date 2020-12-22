package com.adverity.challenge.api.unit;

import com.adverity.challenge.api.controller.WebQuery;
import com.adverity.challenge.api.results.Table;
import com.adverity.challenge.api.utils.QueryValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;

import static com.adverity.challenge.api.controller.WebQuery.Group.CAMPAIGN;
import static com.adverity.challenge.api.controller.WebQuery.Group.DATASOURCE;
import static com.adverity.challenge.api.controller.WebQuery.Query.of;
import static org.junit.jupiter.api.Assertions.*;

class QueryValidationTests {

    @Test
    void testBasicQuery() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks","")).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertTrue(queryIsValid);
    }

    @Test
    void testInvalidMetric() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("views")).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertFalse(queryIsValid);
    }

    @Test
    void testOperationNotPresent() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks")).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertTrue(queryIsValid);
    }

    @Test
    void testInvalidOperation() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks", "top")).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertFalse(queryIsValid);
    }

    @Test
    void testFiltersWithWrongDate() throws Exception {
        // given
        var startingDate = LocalDate.of(2019,11,20);
        var aDateBeforeStartingDate = LocalDate.of(2019,11,11);
        var webQuery = new WebQuery.Builder(of("clicks")).fromDate(startingDate).toDate(aDateBeforeStartingDate).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertTrue(aDateBeforeStartingDate.isBefore(startingDate));
        assertFalse(queryIsValid);
    }

    @Test
    void whenFiltersOnDateStartAndEndOnTheSameDayTheQueryIsValid() throws Exception {
        // given
        var startingDate = LocalDate.of(2019,11,11);
        var webQuery = new WebQuery.Builder(of("clicks")).fromDate(startingDate).toDate(startingDate).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertTrue(queryIsValid);
    }

    @Test
    void whenGroupByIsPresentThenAggregationOperationMustBePresent() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks")).setGroupBy(List.of(DATASOURCE)).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertFalse(queryIsValid);
    }

    @Test
    void whenMetricIsCTRThenGroupByCanBeEmpty() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("ctr")).build();
        // when
        var queryIsValid = QueryValidator.isValid(webQuery);
        // then
        assertTrue(queryIsValid);
    }

}
