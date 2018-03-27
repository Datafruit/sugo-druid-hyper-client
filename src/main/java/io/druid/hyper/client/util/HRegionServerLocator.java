package io.druid.hyper.client.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HRegionServerLocator {

    private static final String QUERY_REGION_SERVER_SCHEMA = "http://%s/druid/hmaster/v1/datasources/serverview/writable/%s";
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final String dataSource;
    private volatile Map<Integer, List<String>> servers = Maps.newConcurrentMap();

    private final String[] hmasters;

    public HRegionServerLocator(String hmaster, String dataSource) {
        Preconditions.checkNotNull(hmaster, "hmaster can not be null.");
        hmasters = StringUtils.split(hmaster, ",");
        Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
        this.dataSource = dataSource;
    }

    public String getServer(int partitionNum) throws IOException {
        if (servers.isEmpty()) {
            queryRegionServers();
        }

        List<String> servers = this.servers.get(String.valueOf(partitionNum));
        return servers == null || servers.isEmpty() ? null : servers.get(0);
    }

    public void reload() throws IOException {
        queryRegionServers();
    }

    private void queryRegionServers() throws IOException {
        String queryRegionServerUrl = String.format(QUERY_REGION_SERVER_SCHEMA, HMasterUtil.getLeader(hmasters), dataSource);
        String serverStr = HttpClientUtil.get(queryRegionServerUrl);
        servers = jsonMapper.readValue(serverStr, Map.class);
    }
}
