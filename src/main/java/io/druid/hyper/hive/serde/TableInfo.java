package io.druid.hyper.hive.serde;

import io.druid.hyper.hive.io.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableInfo {
  public static final String COLUMN_TYPES_SPLIT = ":";

  private final List<String> columnNames = new ArrayList<>();
  private final List<TypeInfo> columnTypes = new ArrayList<>();
  private final List<String> comments = new ArrayList<>();
  private final Map<String, TypeInfo> columnTypeMap = new HashMap<>();
  private final List<ObjectInspector> inspectors = new ArrayList<>();
  private boolean timestampAdded = false;

  public TableInfo() {
    // Timestamp column
    addColumn(Constants.TIME_COLUMN_NAME, TypeInfoFactory.timestampTypeInfo);
    timestampAdded = true;
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

  public void addColumn(String col, PrimitiveTypeInfo type) {
    addColumn(col, type, "" , false);
  }
  public void addColumn(String col, PrimitiveTypeInfo type, boolean hasMultipleValues) {
    addColumn(col, type, "" ,hasMultipleValues);
  }

  public void addColumn(String col, PrimitiveTypeInfo type, String comment, boolean hasMultipleValues) {
    if (Constants.TIME_COLUMN_NAME.equals(col) && timestampAdded) {
      return;
    }
    columnNames.add(col);
    columnTypes.add(type);
    comments.add(comment);
    columnTypeMap.put(col.toLowerCase(), type);
    AbstractPrimitiveWritableObjectInspector inspector = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type);
    if (hasMultipleValues){
      inspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(inspector));
    }
    else {
      inspectors.add(inspector);
    }

  }

  @Override public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("columns:").append(columnNames);
    builder.append(", types:").append(columnTypes);
    return builder.toString();
  }
}
