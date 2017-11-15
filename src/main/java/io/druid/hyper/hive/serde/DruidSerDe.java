/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.hyper.hive.serde;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.hyper.hive.io.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.*;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

/**
 * DruidSerDe that is used to  deserialize objects from a Druid data source.
 */
@SerDeSpec(schemaProps = { Constants.DRUID_DATA_SOURCE })
public class DruidSerDe extends AbstractSerDe {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidSerDe.class);

  private int numConnection;
  private Period readTimeout;

  private String[] columns;
  private PrimitiveTypeInfo[] types;
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    final List<String> columnNames = new ArrayList<>();
    final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
    List<ObjectInspector> inspectors = new ArrayList<>();

    // Druid query
    String druidQuery = properties.getProperty(Constants.DRUID_QUERY_JSON);
    if (druidQuery == null) {
      // No query. Either it is a CTAS, or we need to create a Druid
      // Segment Metadata query that retrieves all columns present in
      // the data source (dimensions and metrics).
      if (!org.apache.commons.lang3.StringUtils
              .isEmpty(properties.getProperty(serdeConstants.LIST_COLUMNS))
              && !org.apache.commons.lang3.StringUtils
              .isEmpty(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES))) {
        columnNames.addAll(Utilities.getColumnNames(properties));

        columnTypes.addAll(Lists.transform(getColumnTypes(properties),
                new Function<String, PrimitiveTypeInfo>() {
                  @Override
                  public PrimitiveTypeInfo apply(String type) {
                    return TypeInfoFactory.getPrimitiveTypeInfo(type);
                  }
                }
        ));
        inspectors.addAll(Lists.transform(columnTypes,
                new Function<PrimitiveTypeInfo, ObjectInspector>() {
                  @Override
                  public ObjectInspector apply(PrimitiveTypeInfo type) {
                    return PrimitiveObjectInspectorFactory
                            .getPrimitiveWritableObjectInspector(type);
                  }
                }
        ));
        columns = columnNames.toArray(new String[columnNames.size()]);
        types = columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
        inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, inspectors);
      } else {
        String dataSource = properties.getProperty(Constants.DRUID_DATA_SOURCE);
        if (dataSource == null) {
          throw new SerDeException("Druid data source not specified; use " +
                  Constants.DRUID_DATA_SOURCE + " in table properties");
        }


        columns = columnNames.toArray(new String[columnNames.size()]);
        types = columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
        inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, inspectors);
      }
    } else {
      // Query is specified, we can extract the results schema from the query


      columns = new String[columnNames.size()];
      types = new PrimitiveTypeInfo[columnNames.size()];
      for (int i = 0; i < columnTypes.size(); ++i) {
        columns[i] = columnNames.get(i);
        types[i] = columnTypes.get(i);
        inspectors
                .add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(types[i]));
      }
      inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("DruidSerDe initialized with\n"
              + "\t columns: " + columnNames
              + "\n\t types: " + columnTypes);
    }
  }


  @Override
  public Class<? extends Writable> getSerializedClass() {
    return DruidWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
              + " can only serialize struct types, but we got: "
              + objectInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> values = soi.getStructFieldsDataAsList(o);
    // We deserialize the result
    Map<String, Object> value = new HashMap<>();
    for (int i = 0; i < columns.length; i++) {
      if (values.get(i) == null) {
        // null, we just add it
        value.put(columns[i], null);
        continue;
      }
      final Object res;
      switch (types[i].getPrimitiveCategory()) {
        case TIMESTAMP:
          res = ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector())
                  .getPrimitiveJavaObject(
                          values.get(i)).getTime();
          break;
        case BYTE:
          res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
          break;
        case SHORT:
          res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
          break;
        case INT:
          res = ((IntObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
          break;
        case LONG:
          res = ((LongObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
          break;
        case FLOAT:
          res = ((FloatObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
          break;
        case DOUBLE:
          res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector())
                  .get(values.get(i));
          break;
        case DECIMAL:
          res = ((HiveDecimalObjectInspector) fields.get(i).getFieldObjectInspector())
                  .getPrimitiveJavaObject(values.get(i)).doubleValue();
          break;
        case STRING:
          res = ((StringObjectInspector) fields.get(i).getFieldObjectInspector())
                  .getPrimitiveJavaObject(
                          values.get(i));
          break;
        default:
          throw new SerDeException("Unknown type: " + types[i].getPrimitiveCategory());
      }
      value.put(columns[i], res);
    }

    return new DruidWritable(value);
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    DruidWritable input = (DruidWritable) writable;
    List<Object> output = Lists.newArrayListWithExpectedSize(columns.length);
    for (int i = 0; i < columns.length; i++) {
      final Object value = input.getValue().get(columns[i]);
      if (value == null) {
        output.add(null);
        continue;
      }
      switch (types[i].getPrimitiveCategory()) {
        case TIMESTAMP:
          output.add(new TimestampWritable(new Timestamp((Long) value)));
          break;
        case BYTE:
          output.add(new ByteWritable(((Number) value).byteValue()));
          break;
        case SHORT:
          output.add(new ShortWritable(((Number) value).shortValue()));
          break;
        case INT:
          output.add(new IntWritable(((Number) value).intValue()));
          break;
        case LONG:
          output.add(new LongWritable(((Number) value).longValue()));
          break;
        case FLOAT:
          output.add(new FloatWritable(((Number) value).floatValue()));
          break;
        case DOUBLE:
          output.add(new DoubleWritable(((Number) value).doubleValue()));
          break;
        case STRING:
          output.add(new Text(value.toString()));
          break;
        default:
          throw new SerDeException("Unknown type: " + types[i].getPrimitiveCategory());
      }
    }
    return output;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  public static List<String> getColumnTypes(Properties props) {
    List<String> names = new ArrayList<String>();
    String colNames = props.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    String[] cols = colNames.trim().split(":");
    if (cols != null) {
      for (String col : cols) {
        if (col != null && !col.trim().equals("")) {
          names.add(col);
        }
      }
    }
    return names;
  }

}
