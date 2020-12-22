package com.adverity.challenge.api.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MapUtils {

    public static Map<String, String> of(String datasource, String campaign, String daily, String clicks, String impressions) {
        Map<String,String> m = new HashMap<>();
        m.put("datasource", datasource);
        m.put("campaign", campaign);
        m.put("daily", daily);
        m.put("clicks", clicks);
        m.put("impressions", impressions) ;
        return m;
    }

}
