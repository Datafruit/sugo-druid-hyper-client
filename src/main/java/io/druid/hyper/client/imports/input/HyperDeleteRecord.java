package io.druid.hyper.client.imports.input;

import com.google.common.base.Preconditions;

import java.util.List;

public class HyperDeleteRecord extends BatchRecord {

    private final List<String> primaryValues;

    public HyperDeleteRecord(
            String dataSource,
            Integer partitionNum,
            List<String> primaryValues) {
        super(BatchRecord.RECORD_ACTION_DELETE, dataSource, partitionNum);
        Preconditions.checkNotNull(primaryValues.size() >= 1, "must specified at least one value.");
        this.primaryValues = primaryValues;
    }

    @Override
    public int rows() {
        return primaryValues.size();
    }

    @Override
    public String toString() {
        return "HyperDeleteRecord{" +
                "dataSource='" + getDataSource() + '\'' +
                "partitionNum='" + getPartitionNum() + '\'' +
                ", primaryValues=" + primaryValues +
                '}';
    }
}
