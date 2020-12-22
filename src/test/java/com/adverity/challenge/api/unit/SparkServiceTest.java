package com.adverity.challenge.api.unit;

import com.adverity.challenge.api.SparkAdapter;
import com.adverity.challenge.api.SparkService;
import com.adverity.challenge.api.controller.WebQuery;
import com.adverity.challenge.api.results.Table;
import com.adverity.challenge.api.utils.QueryValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static com.adverity.challenge.api.SparkService.INVALID_QUERY;
import static com.adverity.challenge.api.controller.WebQuery.Query.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class SparkServiceTest {

    public static String testDatafile = "src/main/resources/sample.csv";

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
     void anInvalidQueryReturnsAnError() {
        // given
        Table table = null;
        try(MockedStatic<QueryValidator> queryValidator = Mockito.mockStatic(QueryValidator.class)) {
            WebQuery webQuery = null;
            queryValidator.when(() -> QueryValidator.isValid(webQuery)).thenReturn(false);

            // when
            table = service.queryDataset(webQuery);
        }

        // then
        assertEquals(1, table.getRows().size());
        assertEquals(INVALID_QUERY, table.getRows().get(0).get(0));
    }

    @Test
    void serviceAsksSparkForDataset() {
        // given
        var spySparkAdapter = Mockito.spy(sparkAdapter);
        var webQuery = new WebQuery.Builder(of("clicks")).build();
        service = new SparkService(spySparkAdapter);

        // when
        Table table = service.queryDataset(webQuery);

        // then
        verify(spySparkAdapter, times(1)).getDataset();
    }

}
