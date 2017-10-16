package io.druid.hyper.client.imports.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.druid.hyper.client.util.HttpClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DataSourceSpecLoader {

    private static final Logger log = LoggerFactory.getLogger(DataSourceSpecLoader.class);
    private static final String QUERY_DATA_SOURCE_SCHEMA = "http://%s/druid/hmaster/v1/datasources/spec/%s";
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final String queryDataSourceUrl;
    private DatasourceSpec datasourceSpec;

    public DataSourceSpecLoader(String hmaster, String dataSource) {
        Preconditions.checkNotNull(hmaster, "hmaster can not be null.");
        Preconditions.checkNotNull(dataSource, "dataSource can not be null.");
        this.queryDataSourceUrl = String.format(QUERY_DATA_SOURCE_SCHEMA, hmaster, dataSource);
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

    private void loadDataSourceSpec() {
        try {
            String resultJson = HttpClientUtil.get(queryDataSourceUrl);
            datasourceSpec = jsonMapper.readValue(resultJson, DatasourceSpec.class);
        } catch (IOException e) {
            log.error("Load data source spec error: " + e);
        }
    }
}
