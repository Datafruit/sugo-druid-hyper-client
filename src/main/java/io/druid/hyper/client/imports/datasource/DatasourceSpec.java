package io.druid.hyper.client.imports.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DatasourceSpec {

    private final String primaryColumn;
    private final int primaryIndex;
    private final String delimiter;
    private final List<String> columns;

    @JsonCreator
    public DatasourceSpec(
            @JsonProperty("primaryColumn") String primaryColumn,
            @JsonProperty("primaryIndex") int primaryIndex,
            @JsonProperty("delimiter") String delimiter,
            @JsonProperty("columns") List<String> columns
    ) {
        this.primaryColumn = primaryColumn;
        this.primaryIndex = primaryIndex;
        this.delimiter = delimiter;
        this.columns = columns;
    }

    @JsonProperty
    public String getPrimaryColumn() {
        return primaryColumn;
    }

    @JsonProperty
    public int getPrimaryIndex() {
        return primaryIndex;
    }

    @JsonProperty
    public String getDelimiter() {
        return delimiter;
    }

    @JsonProperty
    public List<String> getColumns() {
        return columns;
    }
}
