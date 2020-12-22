package com.adverity.challenge.api.integrations;

import com.adverity.challenge.api.controller.WebQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.minidev.json.JSONArray;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import java.time.LocalDate;
import java.util.List;

import static com.adverity.challenge.api.controller.WebQuery.Group.DATASOURCE;
import static com.adverity.challenge.api.controller.WebQuery.Query.of;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ActiveProfiles("test")
@SpringBootTest
@AutoConfigureMockMvc
class ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private ObjectMapper mapper;
    private final String uri = "http://localhost:8080/query";

    {
        mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
    }

    @Test
    void queryingClicksShouldReturnClicksOfAllLines() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks")).build();

        // when
        MvcResult result = prepareQuery(webQuery)
        // then
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rows", hasSize(9)))
                .andExpect(jsonPath("$.columns", hasSize(5)))
                .andExpect(jsonPath("$.rows").value(hasItem(contains("Google Ads", "Adventmarkt Touristik", "2019-11-12", "7", "22425"))))
                .andReturn();
    }

    private ResultActions prepareQuery(WebQuery webQuery) throws Exception {
        return this.mockMvc
                .perform(post("/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(webQuery)));
    }

    @Test
    void queryingSumOfClicksGroupedByDatasourceShouldReturnTwoLines() throws Exception {
        // given
        var webQuery = new WebQuery.Builder(of("clicks","sum"))
                .setGroupBy(List.of(DATASOURCE))
                .build();

        // when
        MvcResult result = prepareQuery(webQuery)
        //then
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rows", hasSize(2)))
                .andExpect(jsonPath("$.columns", hasSize(2)))
                .andExpect(jsonPath("$.rows").value(hasItem(contains("Twitter Ads", "17"))))
                .andExpect(jsonPath("$.rows").value(hasItem(contains("Google Ads", "120"))))
                .andReturn();

    }

    @Test
    void testQuerySumClicksWithDateFilter() throws Exception {
        // given
        var fromDate = LocalDate.of(2019,2,7);
        var toDate = LocalDate.of(2019,11,12);
        var webQuery = new WebQuery.Builder(of("clicks"))
                .fromDate(fromDate)
                .toDate(toDate)
                .build();
        // when
        MvcResult result = prepareQuery(webQuery)
        // then
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rows", hasSize(4)))
                .andExpect(jsonPath("$.rows[*][2]",new DateRangeMatcher(fromDate, toDate)))
                .andReturn();
    }


    private class DateRangeMatcher extends BaseMatcher<LocalDate> {

        private LocalDate from;
        private LocalDate to;
        private int misMatchAtIndex;

        DateRangeMatcher(LocalDate from, LocalDate to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean matches(Object item) {
            JSONArray rawData = (JSONArray) item;
            for (Object raw : rawData) {
                LocalDate parsed = LocalDate.parse(raw.toString());
                if (!(parsed.isBefore(to) || parsed.isEqual(to))
                        ||
                    !(parsed.isAfter(from) || parsed.isEqual(from))) {
                    misMatchAtIndex = rawData.indexOf(raw);
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(String.format("All DateTime fields from %s to %s, mismatch at index %d",
                    from, to, misMatchAtIndex));
        }
    }
}
