package io.druid.hyper.client.exports.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ScanQuery {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    /** changeless **/
    @JsonProperty
    private String queryType = "lucene_scan";
    @JsonProperty
    private String resultFormat = "compactedList";
    @JsonProperty
    private int batchSize = 1000;
    @JsonProperty
    private List<String> intervals = Arrays.asList("1000/3000");
    @JsonProperty
    private Map context = ImmutableMap.of("timeout", 900000);

    /** dynamic **/
    @JsonProperty
    private String dataSource;
    @JsonProperty
    private List<String> columns;
    @JsonProperty
    private AndDimFilter filter;
    @JsonProperty
    private int limit;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String dataSource;
        private List<String> columns;
        private int limit = Integer.MAX_VALUE;
        private AndDimFilter filter;

        public Builder select(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder from(String dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder where(FilterType... filterTypes) {
            filter = new AndDimFilter(filterTypes);
            return this;
        }

        public ScanQuery build() {
            Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
            Preconditions.checkNotNull(columns, "columns can not be null.");
            Preconditions.checkArgument(columns.size() >= 1, "must specified at least one column.");
            Preconditions.checkArgument(limit > 0, "count must be greater than 0.");

            return new ScanQuery(dataSource, columns, limit, filter);
        }
    }

    private ScanQuery(String dataSource, List<String> columns, int limit, AndDimFilter filter) {
        this.dataSource = dataSource;
        this.columns = columns;
        this.limit = limit;
        this.filter = filter;
    }

    @Override
    public String toString() {
        try {
            return jsonMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static Filter dimension(String dimension) {
        return new Filter(dimension);
    }

    public static FilterType and(FilterType... filterTypes) {
        AndDimFilter andDimFilter = new AndDimFilter(filterTypes);
        return andDimFilter;
    }

    public static FilterType or(FilterType... filterTypes) {
        OrDimFilter orDimFilter = new OrDimFilter(filterTypes);
        return orDimFilter;
    }

    public static class Filter {
        String dimension;
        private Filter(String dimension) {
            Preconditions.checkNotNull(dimension, "dimension can not be null.");
            this.dimension = dimension;
        }

        public FilterType equal(Object value) {
            return new EqualDimFilter(dimension, value);
        }

        public FilterType notEqual(Object value) {
            return new NotEqualDimFilter(dimension, value);
        }

        public FilterType in(Object... values) {
            return new InDimFilter(dimension, values);
        }

        public FilterType notIn(Object... values) {
            return new NotInDimFilter(dimension, values);
        }

        public FilterType greaterThan(Object value) {
            return new GreaterThanDimFilter(dimension, value);
        }

        public FilterType greaterThanEqual(Object value) {
            return new GreaterThanEqualDimFilter(dimension, value);
        }

        public FilterType lessThan(Object value) {
            return new LessThanDimFilter(dimension, value);
        }

        public FilterType lessThanEqual(Object value) {
            return new LessThanEqualDimFilter(dimension, value);
        }

        public FilterType lookup(Object value) {
            return new LookupDimFilter(dimension, value);
        }
    }

    private static class AndDimFilter extends FilterType {
        @JsonProperty
        List<FilterType> fields = new ArrayList<FilterType>();

        public AndDimFilter(FilterType... filterTypes) {
            Preconditions.checkArgument(filterTypes.length > 0, "must be specified at least one value.");
            this.type = "and";
            this.fields = Lists.newArrayList(filterTypes);
        }
    }

    private static class OrDimFilter extends FilterType {
        @JsonProperty
        List<FilterType> fields = new ArrayList<FilterType>();

        public OrDimFilter(FilterType... filterTypes) {
            Preconditions.checkArgument(filterTypes.length > 0, "must be specified at least one value.");
            this.type = "or";
            this.fields = Lists.newArrayList(filterTypes);
        }
    }

    private static class DimFilter extends FilterType {
        @JsonProperty
        String dimension;
    }

    public static class FilterType {
        @JsonProperty
        String type;
    }

    private static class EqualDimFilter extends DimFilter {
        @JsonProperty
        Object value;

        public EqualDimFilter(String dimension, Object value) {
            this.type = "selector";
            this.dimension = dimension;
            this.value = value;
        }
    }

    private static class NotEqualDimFilter extends FilterType {
        @JsonProperty
        EqualDimFilter field;

        public NotEqualDimFilter(String dimension, Object value) {
            this.field = new EqualDimFilter(dimension, value);
            this.type = "not";
        }
    }

    private static class InDimFilter extends DimFilter {
        @JsonProperty
        List values = new ArrayList<Object>();

        public InDimFilter(String dimension, Object... values) {
            Preconditions.checkArgument(values.length > 0, "must be specified at least one value.");
            this.type = "in";
            this.dimension = dimension;
            this.values = Lists.newArrayList(values);
        }
    }

    private static class NotInDimFilter extends FilterType {
        @JsonProperty
        InDimFilter field;

        public NotInDimFilter(String dimension, Object... values) {
            Preconditions.checkArgument(values.length > 0, "must be specified at least one value.");
            field = new InDimFilter(dimension, values);
            this.type = "not";
        }
    }

    private static class GreaterThanEqualDimFilter extends BoundDimFilter {
        @JsonProperty
        Object lower;

        public GreaterThanEqualDimFilter(String dimension, Object value) {
            this.dimension = dimension;
            this.lower = value;
        }
    }

    private static class GreaterThanDimFilter extends GreaterThanEqualDimFilter {
        @JsonProperty
        boolean lowerStrict;

        public GreaterThanDimFilter(String dimension, Object value) {
            super(dimension, value);
            lowerStrict = true;
        }
    }

    private static class LessThanEqualDimFilter extends BoundDimFilter {
        @JsonProperty
        Object upper;

        public LessThanEqualDimFilter(String dimension, Object value) {
            this.dimension = dimension;
            this.upper = value;
        }
    }

    private static class LessThanDimFilter extends LessThanEqualDimFilter {
        @JsonProperty
        boolean upperStrict;

        public LessThanDimFilter(String dimension, Object value) {
            super(dimension, value);
            upperStrict = true;
        }
    }

    private static class BoundDimFilter extends DimFilter {
        public BoundDimFilter() {
            this.type = "bound";
        }
    }

    private static class LookupDimFilter extends DimFilter {
        @JsonProperty
        Object lookup;

        public LookupDimFilter(String dimension, Object value) {
            this.type = "lookup";
            this.dimension = dimension;
            this.lookup = value;
        }
    }

}
