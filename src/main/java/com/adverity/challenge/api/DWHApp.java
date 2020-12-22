package com.adverity.challenge.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.Serializable;

import static org.apache.spark.sql.functions.sum;

@SpringBootApplication
public class DWHApp implements Serializable {

    public static void main(String[] args) {
        SpringApplication.run(DWHApp.class, args);
    }

}