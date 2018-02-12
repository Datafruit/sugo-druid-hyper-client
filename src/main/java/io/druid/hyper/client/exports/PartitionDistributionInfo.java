package io.druid.hyper.client.exports;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;

public class PartitionDistributionInfo {

  @JsonProperty
  private int partition;
  @JsonProperty
  private String version;
  @JsonProperty
  private String interval;
  @JsonProperty
  private List<String> servers;

  public int getPartition() {
    return partition;
  }

  public String getVersion() {
    return version;
  }

  public String getInterval() {
    return interval;
  }

  public List<String> getServers() {
    return servers;
  }

  public void setServers(List<String> servers) {
    this.servers = servers;
  }

  @Override public int hashCode() {
    int result = partition;
    result = 31 * result + version.hashCode();
    result = 31 * result + interval.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionDistributionInfo that = (PartitionDistributionInfo) o;

    if (partition != that.partition) {
      return false;
    }
    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }
    if (interval != null ? !interval.equals(that.interval) : that.interval != null) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("{interval:%s, version:%s, partition:%d}, servers:%s", interval, version, partition, servers);
  }
}
