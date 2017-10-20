package io.druid.hyper.client.imports.input;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HyperAddRecord extends BatchRecord {

    /**
     * The values of this batch record, each element represents a single row,
     * which is csv-file-like pattern.
     */
    private final List<String> values;

    public HyperAddRecord(
            String dataSource,
            Integer partitionNum,
            List<String> values) {
        super(BatchRecord.RECORD_ACTION_ADD, dataSource, partitionNum);
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

    @Override
    public String toString() {
        return "HyperAddRecord{" +
                "dataSource='" + getDataSource() + '\'' +
                "partitionNum='" + getPartitionNum() + '\'' +
                ", values=" + values +
                '}';
    }
}
