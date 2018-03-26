package io.druid.hyper.hive.serde;

import io.druid.hyper.hive.io.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableInfo {
  public static final String COLUMN_TYPES_SPLIT = ":";

  private final List<String> columnNames = new ArrayList<>();
  private final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
  private final List<String> comments = new ArrayList<>();
  private final Map<String, PrimitiveTypeInfo> columnTypeMap = new HashMap<>();
  private final List<ObjectInspector> inspectors = new ArrayList<>();
  private boolean timestampAdded = false;

  public TableInfo() {
    // Timestamp column
    addColumn(Constants.TIME_COLUMN_NAME, TypeInfoFactory.timestampTypeInfo);
    timestampAdded = true;
  }

  public boolean containColumn(String column) {
    return columnNames.contains(column);
  }

  public PrimitiveTypeInfo getColumnType(String columnName) {
    PrimitiveTypeInfo type = columnTypeMap.get(columnName.toLowerCase());
    if (type != null) {
      return type;
    }
    return TypeInfoFactory.stringTypeInfo;
  }

  public PrimitiveTypeInfo[] getColumnTypes() {
    return columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<ObjectInspector> getInspectors() {
    return inspectors;
  }

  public List<String> getComments() {
    return comments;
  }


  public String[] getColumnArray() {
    return columnNames.toArray(new String[columnNames.size()]);
  }

  public void addColumn(String col) {
    String[] tmp = col.split(COLUMN_TYPES_SPLIT);
    PrimitiveTypeInfo type = TypeInfoFactory.getPrimitiveTypeInfo(tmp[1]);
    addColumn(tmp[0], type, col);
  }

  public void addColumn(String col, String typeName) {
    PrimitiveTypeInfo type = DruidSerDeUtils.convertDruidToHiveType(typeName);
    addColumn(col, type);
  }

  public void addColumn(String col, PrimitiveTypeInfo type) {
    addColumn(col, type, col + COLUMN_TYPES_SPLIT + type.getTypeName());
  }

  public void addColumn(String col, PrimitiveTypeInfo type, String comment) {
    if (Constants.TIME_COLUMN_NAME.equals(col) && timestampAdded) {
      return;
    }
    columnNames.add(col);
    columnTypes.add(type);
    comments.add(comment);
    columnTypeMap.put(col.toLowerCase(), type);
    inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
  }

  @Override public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("columns:").append(columnNames);
    builder.append(", types:").append(columnTypes);
    return builder.toString();
  }
}
