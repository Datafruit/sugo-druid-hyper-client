package io.druid.hyper.client.imports;

import java.util.ArrayList;
import java.util.List;

public class ValueAndFlag {

  private List<String> valuesList;
  private List<boolean[]> appendFlagsList;

  public ValueAndFlag(){
    valuesList = new ArrayList<>();
    appendFlagsList = new ArrayList<>();
  }

  public List<boolean[]> getAppendFlagsList() {
    return appendFlagsList;
  }

  public List<String> getValuesList() {
    return valuesList;
  }

  public void addValues(String values) {
    valuesList.add(values);
  }

  public void addAppendFlags(boolean[] flags) {
    appendFlagsList.add(flags);
  }

  public void clear() {
    valuesList.clear();
    appendFlagsList.clear();
  }
}
