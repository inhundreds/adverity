package com.adverity.challenge.api.controller;


import com.adverity.challenge.api.SparkService;
import com.adverity.challenge.api.results.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueryController {

    @Autowired
    private SparkService service;

    @PostMapping(path = "/query",
                    headers = "Accept=*/*",
                    produces = "application/json",
                    consumes ="application/json")
    public Table query(@RequestBody WebQuery query) {
        return service.queryDataset(query);
    }

}