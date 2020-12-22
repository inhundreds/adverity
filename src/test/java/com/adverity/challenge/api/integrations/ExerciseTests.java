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

import static com.adverity.challenge.api.controller.WebQuery.Group.CAMPAIGN;
import static com.adverity.challenge.api.controller.WebQuery.Group.DATASOURCE;
import static com.adverity.challenge.api.controller.WebQuery.Query.of;
import static com.adverity.challenge.api.unit.SparkServiceTest.testDatafile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExerciseTests {

    private static final Logger log = LoggerFactory.getLogger(CTRTests.class);

    private static SparkAdapter sparkAdapter;
    private SparkService service;

    @BeforeAll
    public static void init () {
        sparkAdapter = new SparkAdapter(testDatafile);
    }

    @BeforeEach
    public void setupService() {
        service = new SparkService(sparkAdapter);
    }

    @Test
    void TotalClicksForAGivenDatasourceForAGivenDateRange() throws Exception {
        // given
        var datasource = "Google Ads";
        var fromDate = LocalDate.of(2019,2,7);
        var toDate = LocalDate.of(2019,11,12);
        var webQuery = new WebQuery.Builder(of("clicks", "sum"))
                                            .setDatasourceFilter(datasource)
                                            .fromDate(fromDate)
                                            .toDate(toDate)
                                            .build();

        // when
        Table table = service.queryDataset(webQuery);
        log.info(table.toString());

        // then
        assertEquals(1, table.getRows().size());
        assertTrue(table.getRows().contains(List.of("7")));
    }

    @Test
    void CTRPerDatasourceAndCampaign() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("ctr"))
                                            .setGroupBy(List.of(DATASOURCE, CAMPAIGN))
                                            .build();
        // when
        Table r = service.queryDataset(webQuery);
        log.info(r.toString());

        // then
        assertEquals(4, r.getRows().size());
        assertTrue(r.getRows().contains(List.of("Google Ads","Adventmarkt Touristik","0.0338848210734122")));
        assertTrue(r.getRows().contains(List.of("Twitter Ads","DE|SN|Skullcandy","7.87037037037037")));
        assertTrue(r.getRows().contains(List.of("Google Ads","GDN_Retargeting","0.1204206030961751")));
        assertTrue(r.getRows().contains(List.of("Google Ads","Firmen Mitgliedschaft","0.0")));
    }

    @Test
    void ImpressionOverTime() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("impressions")).build();

        // when
        Table r = service.queryDataset(webQuery);
        log.info(r.toString());

        // then
        assertEquals(9, r.getRows().size());
        assertTrue(r.getRows().contains(List.of("Google Ads","Adventmarkt Touristik","2019-11-12","7","22425")));
        assertTrue(r.getRows().contains(List.of("Twitter Ads","DE|SN|Skullcandy","2019-02-08","4","90")));
    }

}
