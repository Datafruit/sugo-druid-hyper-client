package io.druid.hyper.client.exports.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ScanQuery {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @JsonProperty
    private String queryType = "lucene_scan";
    @JsonProperty
    private String resultFormat = "compactedList";
    @JsonProperty
    private int batchSize = 1000;
    @JsonProperty
    private List<String> intervals = Arrays.asList("1000/3000");
    @JsonProperty
    private Map<String, Object> context = Maps.newHashMap();

    @JsonProperty
    private String dataSource;
    @JsonProperty
    private List<String> columns;
    @JsonProperty
    private int limit;

    public ScanQuery(String dataSource, List<String> columns, int count) {
        Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
        Preconditions.checkNotNull(columns, "columns can not be null.");
        Preconditions.checkArgument(columns.size() >= 1, "must specified at least one column.");
        Preconditions.checkArgument(count > 1, "count must be greater than 0.");

        this.dataSource = dataSource;
        this.columns = columns;
        this.limit = count;
        context.put("timeout", 60000);
    }

    @Override
    public String toString() {
        try {
            return jsonMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
