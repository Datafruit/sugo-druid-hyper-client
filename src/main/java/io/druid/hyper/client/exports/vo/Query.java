package io.druid.hyper.client.exports.vo;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface Query {
    static ObjectMapper jsonMapper = new ObjectMapper();
    static int DEFAULT_TIMEOUT = 900000;
    static int DEFAULT_LIMIT = Integer.MAX_VALUE;

    String getDataSource();
    int getLimit();
    void setLimit(int limit);
    void setIntervals(Object intervals);
    String queryString();
}
