package io.druid.hyper.client.imports.input;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;

public abstract class BatchRecord {

    public static final String RECORD_ACTION_ADD = "A";
    public static final String RECORD_ACTION_UPDATE = "U";
    public static final String RECORD_ACTION_DELETE = "D";

    private final String action;
    private final String dataSource;
    private final Integer partitionNum;

    public BatchRecord(
            String action,
            String dataSource,
            Integer partitionNum) {
        Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
        Preconditions.checkNotNull(partitionNum, "partitionNum can not be null.");

        this.action = action;
        this.dataSource = dataSource;
        this.partitionNum = partitionNum;
    }

    public abstract int rows();

    protected void checkSizeMathched(List<String> columns, List<String> columnValues) throws Exception {
        if (columns.size() != columnValues.size()) {
            throw new Exception("Column size and value size are not matched. Column size are: "
                    + columns.size() + ", and value size are: " + columnValues.size());
        }
    }

    protected int getPrimaryIndex(List<String> columns, String primaryColumn) throws Exception {
        int i = 0;
        for (String column : columns) {
            if (column.equals(primaryColumn)) {
                return i;
            }
            i++;
        }
        throw new Exception("Primary column '" + primaryColumn + "' not contained in the submit record.");
    }

    @JsonProperty
    public String getAction() {
        return action;
    }

    @JsonProperty
    public String getDataSource() {
        return dataSource;
    }

    @JsonProperty
    public Integer getPartitionNum() {
        return partitionNum;
    }

    @Override
    public String toString() {
        return "BatchRecord{" +
                "action='" + action + '\'' +
                "dataSource='" + dataSource + '\'' +
                "partitionNum='" + partitionNum + '\'' +
                '}';
    }

}
