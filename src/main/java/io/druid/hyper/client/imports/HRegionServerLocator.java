package io.druid.hyper.client.imports;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.hyper.client.util.HttpClientUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HRegionServerLocator {

    private static final String QUERY_REGION_SERVER_SCHEMA = "http://%s/druid/hmaster/v1/datasources/serverview/writable/%s";
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final String queryRegionServerUrl;
    private volatile Map<Integer, List<String>> partitionNumAndServers = Maps.newConcurrentMap();

    public HRegionServerLocator(String hmaster, String dataSource) {
        Preconditions.checkNotNull(hmaster, "hmaster can not be null.");
        Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
        this.queryRegionServerUrl = String.format(QUERY_REGION_SERVER_SCHEMA, hmaster, dataSource);
    }

    public String getServer(int partitionNum) throws IOException {
        if (partitionNumAndServers.isEmpty()) {
            queryRegionServers();
        }

        List<String> servers = partitionNumAndServers.get(String.valueOf(partitionNum));
        return servers == null || servers.isEmpty() ? null : servers.get(0);
    }

    public int getPartitions() throws IOException {
        if (partitionNumAndServers.isEmpty()) {
            queryRegionServers();
        }
        return partitionNumAndServers.size();
    }

    public void reload() throws IOException {
        queryRegionServers();
    }

    private void queryRegionServers() throws IOException {
        String serverStr = HttpClientUtil.get(queryRegionServerUrl);
        partitionNumAndServers = jsonMapper.readValue(serverStr, Map.class);
    }
}
