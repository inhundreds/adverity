package com.adverity.challenge.api.results;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@ToString
@AllArgsConstructor
public class Table {

    @Getter
    public List<List<String>> rows = null;

    @Getter
    public List<String> columns = null;

}
