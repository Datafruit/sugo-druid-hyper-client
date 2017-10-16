package io.druid.hyper.client.imports.input;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

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
