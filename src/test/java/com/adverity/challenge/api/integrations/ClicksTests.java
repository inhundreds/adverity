package com.adverity.challenge.api.integrations;


import com.adverity.challenge.api.SparkAdapter;
import com.adverity.challenge.api.SparkService;
import com.adverity.challenge.api.controller.WebQuery;
import com.adverity.challenge.api.results.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;

import static com.adverity.challenge.api.controller.WebQuery.Query.of;
import static com.adverity.challenge.api.controller.WebQuery.Group.DATASOURCE;
import static com.adverity.challenge.api.controller.WebQuery.Group.CAMPAIGN;
import static com.adverity.challenge.api.unit.SparkServiceTest.testDatafile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClicksTests {

    private static final Logger log = LoggerFactory.getLogger(ClicksTests.class);

    private static SparkAdapter sparkAdapter;
    private SparkService service;

    @BeforeAll
    public static void init () {
        sparkAdapter = new SparkAdapter(testDatafile);
    }

    @BeforeEach
    public void setupService () {
        service = new SparkService(sparkAdapter);
    }

    @Test
    void queryingMetricWithoutOperationShouldReturnAllTheRows () throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks")).build();

        // when
        Table table = service.queryDataset(webQuery);

        // then
        assertEquals(9, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("Google Ads","Adventmarkt Touristik","2019-11-12","7", "22425")));
        log.info(table.toString());
    }

    @Test
     void queryingAggregationOperationGroupedByADimensionShouldReturnARowForEachDatasource() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks","sum"))
                .setGroupBy(List.of(DATASOURCE))
                .build();

        // when
        Table table = service.queryDataset(webQuery);

        // then
        assertEquals(2, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("Twitter Ads","17")));
        assertTrue(table.getRows().contains(List.of("Google Ads","120")));
        assertTrue(table.getColumns().contains(DATASOURCE.getFieldName()));
        log.info(table.toString());
    }


    @Test
    void queryingAggregationOperationGroupedByTwoDimensionShouldReturnARowForEachCampaign() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks","sum"))
                                            .setGroupBy(List.of(DATASOURCE, CAMPAIGN))
                                            .build();
        // when
        Table table = service.queryDataset(webQuery);

        //then
        assertEquals(4, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("Google Ads", "Adventmarkt Touristik", "23")));
        assertTrue(table.getRows().contains(List.of("Twitter Ads", "DE|SN|Skullcandy", "17")));
        assertTrue(table.getRows().contains(List.of("Google Ads", "GDN_Retargeting", "97")));
        assertTrue(table.getRows().contains(List.of("Google Ads", "Firmen Mitgliedschaft", "0")));
        log.info(table.toString());
    }

    @Test
    void queryingMetricWithDateRangeShouldReturnAllRowWithDailyInsideTheRange() throws Exception {
        // given
        var fromDate = LocalDate.of(2019,2,7);
        var toDate = LocalDate.of(2019,11,12);
        var webQuery = new WebQuery.Builder(of("clicks"))
                                            .fromDate(fromDate)
                                            .toDate(toDate)
                                            .build();

        // when
        Table table = service.queryDataset(webQuery);

        // then
        assertEquals(4, table.getRows().size());
        assertEquals(0, getHowManyRowOutsideDateRange(fromDate, toDate, table));

        log.info(table.toString());
    }

    private long getHowManyRowOutsideDateRange(LocalDate fromDate, LocalDate toDate, Table table) {
        return table.getRows().stream()
                .filter(row -> LocalDate.parse(row.get(2)).isAfter(toDate))
                .filter(row -> LocalDate.parse(row.get(2)).isBefore(fromDate)).count();
    }

    @Test
    void queryingMetricWithDateRangeOfOneDayShouldReturnAllRowWithDailyInsideTheRange() throws Exception {
        // given
        var fromDate = LocalDate.of(2019,11,12);
        var webQuery = new WebQuery.Builder(of("clicks"))
                .fromDate(fromDate)
                .toDate(fromDate)
                .build();

        // when
        Table table = service.queryDataset(webQuery);

        // then
        assertEquals(1, table.getRows().size());
        assertEquals(0, getHowManyRowOutsideDateRange(fromDate, fromDate, table));

        log.info(table.toString());
    }

    @Test
    void queryingMetricWithMaxOperationShouldReturnOneRow() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks","max")).build();

        // when
        Table table = service.queryDataset(webQuery);

        // then
        assertEquals(1, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("32")));
        log.info(table.toString());
    }
}

