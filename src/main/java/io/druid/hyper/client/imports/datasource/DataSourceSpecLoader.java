package io.druid.hyper.client.imports.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.druid.hyper.client.util.HMasterUtil;
import io.druid.hyper.client.util.HttpClientUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DataSourceSpecLoader {

    private static final Logger log = LoggerFactory.getLogger(DataSourceSpecLoader.class);
    private static final String QUERY_DATA_SOURCE_SCHEMA = "http://%s/druid/hmaster/v1/datasources/spec/%s";
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final String dataSource;
    private final String[] hmasters;
    private DatasourceSpec datasourceSpec;

    public DataSourceSpecLoader(String hmaster, String dataSource) {
        Preconditions.checkNotNull(hmaster, "hmaster can not be null.");
        hmasters = StringUtils.split(hmaster, ",");
        Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
        this.dataSource = dataSource;
    }

    public String getPrimaryColumn() {
        if (datasourceSpec == null) {
            loadDataSourceSpec();
        }
        return datasourceSpec.getPrimaryColumn();
    }

    public int getPrimaryIndex() {
        if (datasourceSpec == null) {
            loadDataSourceSpec();
        }
        return datasourceSpec.getPrimaryIndex();
    }

    public String getDelimiter() {
        if (datasourceSpec == null) {
            loadDataSourceSpec();
        }
        return datasourceSpec.getDelimiter();
    }

    public List<String> getColumns() {
        if (datasourceSpec == null) {
            loadDataSourceSpec();
        }
        return datasourceSpec.getColumns();
    }

    public int getPartitions() throws IOException {
        if (datasourceSpec == null) {
            loadDataSourceSpec();
        }
        return datasourceSpec.getPartitions();
    }

    private void loadDataSourceSpec() {
        try {
            String queryDataSourceUrl = String.format(QUERY_DATA_SOURCE_SCHEMA, HMasterUtil.getLeader(hmasters), dataSource);
            String resultJson = HttpClientUtil.get(queryDataSourceUrl);
            datasourceSpec = jsonMapper.readValue(resultJson, DatasourceSpec.class);
        } catch (IOException e) {
            log.error("Load data source spec error: " + e);
        }
    }
}
