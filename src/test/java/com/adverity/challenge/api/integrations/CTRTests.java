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

import java.util.List;

import static com.adverity.challenge.api.controller.WebQuery.Group.CAMPAIGN;
import static com.adverity.challenge.api.controller.WebQuery.Group.DATASOURCE;
import static com.adverity.challenge.api.controller.WebQuery.Query.of;
import static com.adverity.challenge.api.unit.SparkServiceTest.testDatafile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CTRTests {

    private static final Logger log = LoggerFactory.getLogger(CTRTests.class);

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
    void queryingCTRShouldReturnOneRowWithCTR() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("ctr")).build();

        // when
        Table table = service.queryDataset(webQuery);
        log.info(table.toString());

        // then
        assertEquals(1, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("0.09216403853398633")));
    }

    @Test
    void queryingCTRWithFiltersShouldExcludeLines() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("ctr"))
                                            .setDatasourceFilter("Google Ads")
                                            .build();

        // when
        Table table = service.queryDataset(webQuery);
        log.info(table.toString());

        // then
        assertEquals(1, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("0.08084510078689232")));
    }

    @Test
    void queryingCTRWithGroupByShouldReturnsNoOfLineAsTheGroupedBySet() throws Exception {
        // give
        var webQuery = new WebQuery.Builder(of("ctr"))
                                            .setGroupBy(List.of(DATASOURCE))
                                            .build();

        // when
        Table table = service.queryDataset(webQuery);
        log.info(table.toString());

        // then
        assertEquals(2, table.getRows().size());
    }

    @Test
    void testQueryCTRWithTwoGroupBy() throws Exception {
        // when
        var webQuery = new WebQuery.Builder(of("ctr"))
                                                .setGroupBy(List.of(DATASOURCE, CAMPAIGN))
                                                .build();

        // when
        Table table = service.queryDataset(webQuery);
        log.info(table.toString());

        // then
        assertEquals(4, table.getRows().size());
    }

}

