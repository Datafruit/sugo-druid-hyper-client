package io.druid.hyper.client.imports;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import io.druid.hyper.client.imports.input.BatchRecord;
import io.druid.hyper.client.util.HttpClientUtil;
import io.druid.hyper.client.util.RetryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

public class DataSendWorker {

    private static final Logger log = LoggerFactory.getLogger(DataSendWorker.class);
    private static final int MAX_TRY_TIMES = 3;
    private static final String SEND_DATA_URL = "http://%s/druid/regionServer/v1/push";
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private final HRegionServerLocator serverLocator;

    public DataSendWorker(String hmaster, String dataSource) {
        this.serverLocator = new HRegionServerLocator(hmaster, dataSource);
    }

    public void send(final BatchRecord batchRecord) throws Exception {
        RetryUtil.retry(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    String reginServer = serverLocator.getServer(batchRecord.getPartitionNum());
                    String sendUrl = String.format(SEND_DATA_URL, reginServer);
                    HttpClientUtil.post(sendUrl, jsonMapper.writeValueAsString(batchRecord));
                    log.info("Send a batch record to host [" + reginServer + "] successfully!");
                    return null;
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
            MAX_TRY_TIMES
        );
    }

    public int getPartitions() throws IOException {
        return serverLocator.getPartitions();
    }
}