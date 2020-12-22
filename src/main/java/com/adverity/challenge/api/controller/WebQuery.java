package com.adverity.challenge.api.controller;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Getter
@Setter
@ToString
public class WebQuery {

    private Query query;
    private Filters filters = new Filters();
    private List<Group> groupBy = new ArrayList<>();

    public WebQuery(Query query, Filters filters, List<Group> groupBy) {
        this.query = query;
        if (filters != null) this.filters = filters;
        if (groupBy != null) this.groupBy = groupBy;
    }

    public static class Builder {

        private Query query;
        private Filters filters = new Filters();
        private List<Group> groupBy = new ArrayList<>();

        public Builder(Query query) {
            this.query = query;
        }

        public Builder setFilters(Filters filters) {
            this.filters = filters;
            return this;
        }
        public Builder setDatasourceFilter(String datasourceFilter) {
            this.filters.setDatasource(Optional.of(datasourceFilter));
            return this;
        }
        public Builder setCampaignFilter(String campaignFilter) {
            this.filters.setCampaign(Optional.of(campaignFilter));
            return this;
        }
        public Builder fromDate(LocalDate from) {
            this.filters.setDateFrom(Optional.of(from));
            return this;
        }
        public Builder toDate(LocalDate to) {
            this.filters.setDateTo(Optional.of(to));
            return this;
        }
        public Builder setGroupBy(List<Group> groupBy) {
            this.groupBy = groupBy;
            return this;
        }

        public WebQuery build() {
            return new WebQuery(query,filters,groupBy);
        }

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    public static class Query {
        private String metric;
        private String operation = "";

        private Query (String metric, String operation) {
            this.metric = metric;
            this.operation = operation;
        }
        public static Query of(String metric) {
            return of(metric, "");
        }
        public static Query of(String metric, String operation) {
            return new Query(metric,operation);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    public static class Filters {
        private Optional<String> datasource = Optional.empty();
        private Optional<String> campaign = Optional.empty();
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private Optional<LocalDate> dateFrom = Optional.empty();
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private Optional<LocalDate> dateTo = Optional.empty();
    }

    public enum Group {
        DATASOURCE("datasource"),
        CAMPAIGN("campaign");

        String fieldName;
        Group(String fieldName) {
            this.fieldName = fieldName;
        }
        public String getFieldName() {
            return fieldName;
        }
    }

}
