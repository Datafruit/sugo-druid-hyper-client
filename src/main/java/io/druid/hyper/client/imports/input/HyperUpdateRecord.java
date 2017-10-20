package io.druid.hyper.client.imports.input;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HyperUpdateRecord extends BatchRecord {

    /**
     * The columns of this batch record
     */
    private List<String> columns;

    /**
     * The values of this batch record, each element represents a single row,
     * which is csv-file-like pattern.
     */
    private final List<String> values;

    public HyperUpdateRecord(
            String dataSource,
            Integer partitionNum,
            List<String> columns,
            List<String> values) {
        super(BatchRecord.RECORD_ACTION_UPDATE, dataSource, partitionNum);
        this.columns = columns;
        this.values = values;
    }

    @Override
    public int rows() {
        return values.size();
    }

    @JsonProperty
    public List<String> getValues() {
        return values;
    }

    @JsonProperty
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "HyperUpdateRecord{" +
                "dataSource='" + getDataSource() + '\'' +
                "partitionNum='" + getPartitionNum() + '\'' +
                ", columns=" + columns +
                ", values=" + values +
                '}';
    }
}
