package io.druid.hyper.client.imports.input;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.hyper.client.imports.DataSender;
import io.druid.hyper.client.imports.ValueAndFlag;

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

    private final List<byte[]> appendFlags;

    public HyperUpdateRecord(
            String dataSource,
            Integer partitionNum,
            List<String> columns,
           ValueAndFlag valueAndFlag
    ) {
        super(BatchRecord.RECORD_ACTION_UPDATE, dataSource, partitionNum);
        this.columns = columns;
        this.values = valueAndFlag.getValuesList();
        this.appendFlags = valueAndFlag.getAppendFlagsList();
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

    @JsonProperty
    public List<byte[]> getAppendFlags() {
        return appendFlags;
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
