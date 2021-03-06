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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.hyper.client.util.HMasterUtil;
import io.druid.hyper.client.util.HttpClientUtil;
import io.druid.hyper.hive.io.Constants;
import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * DruidSerDe that is used to  deserialize objects from a Druid data source.
 */
@SerDeSpec(schemaProps = {Constants.DRUID_DATA_SOURCE})
public class DruidSerDe extends AbstractSerDe {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidSerDe.class);
  public static final ObjectMapper objectMapper = new ObjectMapper();

  private String[] columns;
  private TypeInfo[] types;
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    TableInfo tableInfo;

    String dataSource = properties.getProperty(Constants.DRUID_DATA_SOURCE);
    if (dataSource == null) {
      throw new SerDeException("Druid data source not specified; use " +
          Constants.DRUID_DATA_SOURCE + " in table properties");
    }

    String masterStr = configuration.get(Constants.HIVE_DRUID_HMASTER_DEFAULT_ADDRESS);
    masterStr = Strings.isNullOrEmpty(masterStr) ? properties.getProperty(Constants.HIVE_DRUID_HMASTER_DEFAULT_ADDRESS) : masterStr;
    Preconditions.checkNotNull(masterStr, "hmaster is null");

    // Infer schema
    List<Pair<String, Pair<String, Boolean>>> columnTypes = submitMetadataRequest(masterStr, dataSource);
    tableInfo = new TableInfo();
    for (Pair<String, Pair<String, Boolean>> columnType : columnTypes) {
      String key = columnType.getKey();
      if (Constants.TIME_COLUMN_NAME.equals(key)) {
        continue;
      }
      Pair<String, Boolean> typePair = columnType.getValue();
      PrimitiveTypeInfo type = DruidSerDeUtils.convertDruidToHiveType(typePair.getKey()); // field type

      tableInfo.addColumn(key, type, typePair.getValue());
    }

    if (tableInfo != null) {
      columns = tableInfo.getColumnArray();
      types = tableInfo.getColumnTypes();
      inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          tableInfo.getColumnNames(),
          tableInfo.getInspectors(),
          tableInfo.getComments()
      );
    }


    if (LOG.isDebugEnabled()) {
      LOG.debug("DruidSerDe initialized with\n" + tableInfo);
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
      TypeInfo typeInfo = types[i];
      if (typeInfo instanceof PrimitiveTypeInfo){
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
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
            throw new SerDeException("Unknown type: " + primitiveTypeInfo.getPrimitiveCategory());
        }
        value.put(columns[i], res);
      } else if (typeInfo instanceof ListTypeInfo){
        List<Object> list = (List<Object>) ((StandardListObjectInspector) fields.get(i).getFieldObjectInspector()).getList(values.get(i));
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) ((ListTypeInfo) typeInfo).getListElementTypeInfo();
        List<Object> resList = new ArrayList<>();
        for (Object ele : list) {
          resList.add(DruidSerDeUtils.convertWritableToJavaObject(ele, primitiveTypeInfo));
        }

        value.put(columns[i], resList);
      }

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
    return null;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  protected List<Pair<String, Pair<String, Boolean>>> submitMetadataRequest(String address, String dataSource)
      throws SerDeException {
    try {

      String response = HttpClientUtil.get(
          String.format("http://%s/druid/hmaster/v1/datasources/dimensions/%s", HMasterUtil.getLeader(StringUtils.split(address,",")), dataSource));

      Map<String, Object> segmentAnalysisList = objectMapper.readValue(
          response,
          new TypeReference<Map<String, Object>>() {
          });
      List<Map<String, Object>> columnMap = (List<Map<String, Object>>) segmentAnalysisList.get("dimensions");
      List<Pair<String, Pair<String, Boolean>>> columnTypes = new ArrayList<>(columnMap.size());
      for (Map<String, Object> entity : columnMap) {
        String type = entity.get("type").toString();
        boolean hasMultipleValues = (boolean) entity.get("hasMultipleValues");
        columnTypes.add(new Pair<>(entity.get("name").toString(), new Pair<>(type.toUpperCase(), hasMultipleValues)));
      }
      if (columnTypes.size() < 1) {
        throw new SerDeException(String.format("No column in DataSource[%s]", dataSource));
      }
      return columnTypes;
    } catch (Exception e) {
      throw new SerDeException(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }
}
