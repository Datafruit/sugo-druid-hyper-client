package io.druid.hyper.client.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.druid.hyper.client.util.HRegionServerLocator;
import io.druid.hyper.client.util.HttpClientUtil;
import io.druid.hyper.client.util.PartitionUtil;
import io.druid.hyper.client.util.RetryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class GetQueryClient {
    private static final Logger log = LoggerFactory.getLogger(GetQueryClient.class);
    private static final String QUERY_URL = "http://%s/druid/v2/?pretty";
    private static final List<String> DEFAULT_COLUMNS = Lists.newArrayList();
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final String dataSource;
    private final HRegionServerLocator serverLocator;

    public GetQueryClient(String hmaster, String dataSource) {
        Preconditions.checkNotNull(hmaster, "server can not be null.");
        Preconditions.checkNotNull(dataSource, "data source can not be null.");
        this.dataSource = dataSource;
        this.serverLocator = new HRegionServerLocator(hmaster, dataSource);
    }

    public Map<String, Object> getRow(String key) throws Exception {
        return getRow(key, DEFAULT_COLUMNS);
    }

    public Map<String, Object> getRow(final String key, final List<String> columns)throws Exception {
        Map<String, Object> res = RetryUtil.retry(
                new Callable<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> call() throws Exception {
                        int partition = serverLocator.getPartitions();
                        int partitionNum = PartitionUtil.getPartitionNum(key, partition);
                        GetQuery getQuery = new GetQuery(dataSource, key, partitionNum, columns);

                        String reginServer = serverLocator.getServer(partitionNum);
                        String sendUrl = String.format(QUERY_URL, reginServer);
                        String jsonData = jsonMapper.writeValueAsString(getQuery);
                        String resultStr = HttpClientUtil.post(sendUrl, jsonData);
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("get row[key=%s] from [%s] successfully!", key, reginServer));
                        }
                        List<Map<String, Object>> resultList = jsonMapper.readValue(resultStr, List.class);
                        if (resultList.size() > 0) {
                            return (Map<String, Object>) (resultList.get(0).get("event"));
                        } else {
                            return Collections.emptyMap();
                        }
                    }
                },
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        serverLocator.reload();
                        return null;
                    }
                },
                new Predicate<Throwable>() {
                    @Override
                    public boolean apply(Throwable input) {
                        if (input == null) {
                            return false;
                        }
                        if (input instanceof IOException) {
                            return true;
                        }
                        if (input instanceof Exception) {
                            return true;
                        }
                        return apply(input.getCause());
                    }
                },
                RetryUtil.MAX_TRY_TIMES
        );

        return res;
    }
}
