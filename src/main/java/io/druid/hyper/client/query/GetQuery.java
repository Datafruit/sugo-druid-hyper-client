package io.druid.hyper.client.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetQuery {

    private static final String DEFAULT_QUERY_INTERVAL = Interval.parse("1000/3000").toString();
    private static final String DEFAULT_VERSION = "0";

    @JsonProperty
    private final String queryType = "lucene_get";
    @JsonProperty
    private String dataSource;
    @JsonProperty
    private MultipleSpecificSegmentSpec intervals;
    @JsonProperty
    private String primaryKey;
    @JsonProperty
    private List<String> columns;
    @JsonProperty
    private Map<String, Object> context = Maps.newHashMap();

    public GetQuery(String dataSource, String primaryKey, int partitionNumber, List<String> columns) {
        this.dataSource = dataSource;
        this.intervals = new MultipleSpecificSegmentSpec(partitionNumber);
        this.primaryKey = primaryKey;
        this.columns = columns;
    }

    private static class MultipleSpecificSegmentSpec {
        @JsonProperty
        private final String type = "segments";
        @JsonProperty
        private final List<SegmentDescriptor> segments = new ArrayList<SegmentDescriptor>(1);

        public MultipleSpecificSegmentSpec(int partitionNumber) {
            this.segments.add(new SegmentDescriptor(partitionNumber));
        }
    }

    private static class SegmentDescriptor {
        @JsonProperty("itvl")
        private final String interval = DEFAULT_QUERY_INTERVAL;
        @JsonProperty("ver")
        private final String version = DEFAULT_VERSION;
        @JsonProperty("part")
        private final int partitionNumber;

        public SegmentDescriptor(int partitionNumber) {
            this.partitionNumber = partitionNumber;
        }
    }

}
