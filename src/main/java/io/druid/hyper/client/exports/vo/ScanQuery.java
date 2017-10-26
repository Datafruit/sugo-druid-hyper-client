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
    private Map context = ImmutableMap.of("timeout", 60000);

    /** dynamic **/
    @JsonProperty
    private String dataSource;
    @JsonProperty
    private List<String> columns;
    @JsonProperty
    private int limit;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String dataSource;
        private List<String> columns;
        private int limit;
        private FilterBuilder filterBuilder;

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

        public FilterBuilder where() {
            this.filterBuilder = new FilterBuilder();
            return filterBuilder;
        }

        public ScanQuery build() {
            Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
            Preconditions.checkNotNull(columns, "columns can not be null.");
            Preconditions.checkArgument(columns.size() >= 1, "must specified at least one column.");
            Preconditions.checkArgument(limit > 1, "count must be greater than 0.");

            return new ScanQuery(dataSource, columns, limit);
        }
    }

    public static class FilterBuilder {
        private List<FieldType> andFields = new ArrayList<>();
        private List<FieldType> orFields = new ArrayList<>();

        public FilterBuilder andEqual(Condition condition) {
            andFields.add(new EqualField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andNotEqual(Condition condition) {
            andFields.add(new NotEqualField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andIn(Condition condition) {
            andFields.add(new InField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andNotIn(Condition condition) {
            andFields.add(new NotInField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andGreaterThan(Condition condition) {
            andFields.add(new GreaterThanField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andGreaterThanEqual(Condition condition) {
            andFields.add(new GreaterThanEqualField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andLessThan(Condition condition) {
            andFields.add(new LessThanField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andLesshanEqual(Condition condition) {
            andFields.add(new LessThanEqualField(condition.getColumn(), condition.getValue()));
            return this;
        }

        public FilterBuilder andLookup(Condition condition) {
            andFields.add(new LookupField(condition.getColumn(), condition.getValue()));
            return this;
        }
    }

    private ScanQuery(String dataSource, List<String> columns, int limit) {
        this.dataSource = dataSource;
        this.columns = columns;
        this.limit = limit;
    }

    @Override
    public String toString() {
        try {
            return jsonMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static class Condition {
        String column;
        Object value;

        public Condition(String column, Object... value) {
            this.column = column;
            this.value = value;
        }

        public String getColumn() {
            return column;
        }

        public Object getValue() {
            return value;
        }
    }

    private static class AndFilter {
        String type = "and";
        List<FieldType> fields = new ArrayList<>();

        public AndFilter() {
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<FieldType> getFields() {
            return fields;
        }

        public void setFields(List<FieldType> fields) {
            this.fields = fields;
        }
    }

    private static class OrFilter {
        String type = "or";
        List<FieldType> fields = new ArrayList<>();

        public OrFilter() {
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<FieldType> getFields() {
            return fields;
        }

        public void setFields(List<FieldType> fields) {
            this.fields = fields;
        }
    }

    private static class Field extends FieldType {
        String dimension;

        public String getDimension() {
            return dimension;
        }

        public void setDimension(String dimension) {
            this.dimension = dimension;
        }
    }

    private static class FieldType {
        String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    private static class EqualField extends Field {
        Object value;
        public EqualField(String dimension, Object value) {
            this.type = "selector";
            this.dimension = dimension;
            this.value = value;
        }
    }

    private static class NotEqualField extends FieldType {
        EqualField field;
        public NotEqualField(String dimension, Object value) {
            super.type = "not";
            this.field = new EqualField(dimension, value);
        }
    }

    private static class InField extends Field {
        List values = new ArrayList<>();
        public InField(String dimension, Object... values) {
            this.type = "in";
            this.dimension = dimension;
            this.values = Lists.newArrayList(values);
        }
    }

    private static class NotInField extends FieldType {
        InField field;
        public NotInField(String dimension, Object... values) {
            super.type = "not";
            field = new InField(dimension, values);
        }
    }

    private static class GreaterThanEqualField extends BoundField {
        Object lower;
        public GreaterThanEqualField(String dimension, Object value) {
            this.dimension = dimension;
            this.lower = value;
        }
    }

    private static class GreaterThanField extends GreaterThanEqualField {
        Boolean lowerStrict = true;
        public GreaterThanField(String dimension, Object value) {
            super(dimension, value);
        }
    }

    private static class LessThanEqualField extends BoundField {
        Object upper;
        public LessThanEqualField(String dimension, Object value) {
            this.dimension = dimension;
            this.upper = value;
        }
    }

    private static class LessThanField extends LessThanEqualField {
        Boolean upperStrict = true;
        public LessThanField(String dimension, Object value) {
            super(dimension, value);
        }
    }

    private static class BoundField extends Field {
        public BoundField() {
            this.type = "bound";
        }
    }

    private static class LookupField extends Field {
        Object lookup;
        public LookupField(String dimension, Object value) {
            this.type = "lookup";
            this.dimension = dimension;
            this.lookup = value;
        }
    }

}
