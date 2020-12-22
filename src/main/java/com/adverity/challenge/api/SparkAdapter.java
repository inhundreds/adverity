package com.adverity.challenge.api;

import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class SparkAdapter implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkAdapter.class);

    SparkSession sparkSession;
    String datafile;

    @Getter
    private Dataset<Row> dataset;

    public SparkAdapter(@Value("${datafile:src/main/resources/sample.csv}") String datafile) {
        setupDatafile(datafile);
        setupSparkSession();
        initDataset();
    }

    private void setupDatafile(String datafile) {
        this.datafile = datafile;
        log.info("Datafile: {}", datafile);
    }

    private void setupSparkSession() {
        sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.executor.instances", "4")
                .config("spark.sql.debug.maxToStringFields", 1000)
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("INFO");
    }



    private void initDataset() {

        dataset = sparkSession.read().option("dateFormat", "MM/dd/yy")
                                .schema("datasource STRING, campaign STRING, daily DATE, clicks LONG, impressions LONG")
                                .format("csv")
                                .load(datafile);
    }

}
