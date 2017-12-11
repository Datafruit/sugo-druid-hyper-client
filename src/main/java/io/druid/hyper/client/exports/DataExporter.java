package io.druid.hyper.client.exports;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.hyper.client.exports.vo.Query;
import io.druid.hyper.client.exports.vo.ScanQuery;
import io.druid.hyper.client.exports.vo.SqlQuery;
import io.druid.hyper.client.util.JsonObjectIterator;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class DataExporter implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DataExporter.class);
    private static final MediaType DEFAULT_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final String SERVER_SCHEMA = "http://%s/druid/v2?pretty";
    private static final String HMASTER_SERVER_SCHEMA = "http://%s/druid/hmaster/v1/datasources/segments/%s";
    private static final String SEPARATOR_COMMA = ",";
    private static final String SEPARATOR_TAB = "\t";
    private static final String SEPARATOR_HIVE = "\001";

    String filePath;
    String server;
    String separator;
    Query query;
    String sql;
    OutputStream outputStream;
    private String plyql;
    private int totalRecord = 0;
    private boolean progressLog = false;

    public static DataExporter local() {
        return new LocalDataExporter();
    }

    public static DataExporter hdfs() {
        return new HdfsDataExporter();
    }

    private void checkAndInitialize() throws Exception {
        Preconditions.checkNotNull(server, "server can not be null.");
        Preconditions.checkState(query != null || !Strings.isNullOrEmpty(sql), "query or sql can not be null.");
        Preconditions.checkState(filePath != null || outputStream != null, "export file or output stream can not be null.");

        if (outputStream == null) {
            log.info("Start to export data from server [" + server + " ] to file [" + filePath + "].");
            init(filePath);
        } else {
            log.info("Start to export data from server [" + server + " ] to stream.");
            init(outputStream);
        }

        query = query != null ? query : new SqlQuery(plyql, sql);
    }

  /**
   * export data from RegionServer, DataExporter#server must be HMaster
   *
   * @throws Exception
   */
  public void export() throws Exception {
        long start = System.currentTimeMillis();
        checkAndInitialize();

        OkHttpClient client = getHttpClient();
        String hmasterUrl = String.format(HMASTER_SERVER_SCHEMA, server, query.getDataSource());
        Request metaRequest = new Request.Builder().url(hmasterUrl).get().build();
        Response metaResponse = client.newCall(metaRequest).execute();
        if (metaResponse.code() != 200) {
            String errorMsg = "Request server failed, please check the server address [" + server
                + "] you specified is correct? The most likely address is something like 'HMasterIp:8086'.";
            throw new RuntimeException(errorMsg);
        }

        List<PartitionDistributionInfo> pdis = Query.jsonMapper.readValue(
            metaResponse.body().byteStream(),
            new TypeReference<List<PartitionDistributionInfo>>() {
            }
        );

        if (pdis.isEmpty()) {
            throw new RuntimeException(String.format("dataSource[%s] is not exists", query.getDataSource()));
        }

        int rowCount = 0;
        int limit = query.getLimit();
        for (PartitionDistributionInfo pdi : pdis) {
            //writable RegionServer will be at first
            List<String> regionServers = pdi.getServers();
            if (regionServers.isEmpty()) {
                log.warn("Partition[%d] of datasource[%s] has no servers", query.getDataSource(), pdi.getPartition());
                continue;
            }
            if (rowCount < limit) {
                query.setLimit(limit - rowCount);
            }
            String queryStr = buildQueryString(pdi);
            for (String regionServer : regionServers) {
                try {
                    RequestBody body = RequestBody.create(DEFAULT_MEDIA_TYPE, queryStr);
                    Request request = (new Request.Builder()).url(String.format(SERVER_SCHEMA, regionServer)).post(body).build();
                    Response response = client.newCall(request).execute();
                    int rtnCode = response.code();
                    if (rtnCode == 200) {
                        int read = readFromResponse(response);
                        rowCount += read;
                        if(progressLog) {
                          log.info("read row[%,d] from [%s], exported row count[%,d]", read, regionServer, rowCount);
                        }
                        break;
                    } else {
                        log.warn("Request server[%s] for Partition[%d] of dataSource[%s] failed, please check the server log",
                            regionServer, pdi.getPartition(), query.getDataSource());
                        continue;
                    }
                } catch (ConnectException ce) {
                    log.warn("Request server[%s] for Partition[%d] of dataSource[%s] failed, please check the server log",
                        regionServer, pdi.getPartition(), query.getDataSource());
                }
            }
            if (rowCount >= limit) {
                break;
            }
        }

        close();

        totalRecord = rowCount;

        long end = System.currentTimeMillis();
        log.info("Export data successfully, row count:" + rowCount + ", cost [" + (end - start) + "] million seconds.");
    }

    private int readFromResponse(Response response) {
        InputStream in = response.body().byteStream();

        int rowCount = 0;
        try {
            JsonObjectIterator iterator = new JsonObjectIterator(in);
            while (iterator.hasNext()) {
                HashMap resultValue = iterator.next();
                if (resultValue != null) {
                    List<List<Object>> events = (List<List<Object>>) resultValue.get("events");
                    for (List<Object> event : events) {
                        writeRow(toLine(event));
                        rowCount++;
                    }
                    flush();
                }
            }
        } catch (Exception e) {
            log.error("Write data to file error: " + e);
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignore) {
                    ignore.printStackTrace();
                }
            }
            if (response != null) {
                response.close();
            }
        }
        return rowCount;
    }

    private String buildQueryString(PartitionDistributionInfo pdi) throws Exception {
        Map<String, Object> intervalMap = new HashMap<>();
        intervalMap.put("type", "segments");

        List<Map<String, Object>> segments = new ArrayList<>();
        Map<String, Object> segment = new HashMap<>();
        segments.add(segment);
        segment.put("itvl", pdi.getInterval());
        segment.put("ver", pdi.getVersion());
        segment.put("part", pdi.getPartition());

        intervalMap.put("segments", segments);
        query.setIntervals(intervalMap);

        return query.queryString();
    }

    private OkHttpClient getHttpClient() {
        return new OkHttpClient.Builder()
            .connectTimeout(1800L, TimeUnit.SECONDS)
            .readTimeout(1800L, TimeUnit.SECONDS).build();
    }

    protected abstract void init(String filePath) throws IOException;

    protected abstract void init(OutputStream outputStream) throws IOException;

    protected abstract void writeRow(String row) throws IOException;

    protected abstract void flush() throws IOException;

    protected String toLine(List<Object> objectList) {
        if (objectList == null || objectList.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Iterator<Object> it = objectList.iterator();
        if (it.hasNext()) {
            sb.append(it.next());
            while (it.hasNext()) {
                sb.append(separator);
                sb.append(it.next());
            }
        }

        return sb.toString();
    }

    public DataExporter fromServer(String server) {
        this.server = server;
        return this;
    }

    public DataExporter toFile(String file) {
        this.filePath = file;
        return this;
    }

    public DataExporter toStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        return this;
    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public DataExporter inCSVFormat() {
        return inFormat(SEPARATOR_COMMA);
    }

    public DataExporter inTSVFormat() {
        return inFormat(SEPARATOR_TAB);
    }

    public DataExporter inHiveFormat() {
        return inFormat(SEPARATOR_HIVE);
    }

    public DataExporter inFormat(String separator) {
        this.separator = separator;
        return this;
    }

    public DataExporter withQuery(ScanQuery query) {
        this.query = query;
        return this;
    }

    public DataExporter withSQL(String sql) {
        this.sql = sql;
        return this;
    }

    public DataExporter usePylql(String plyql) {
        this.plyql = plyql;
        return this;
    }

    public DataExporter progressLog() {
      this.progressLog = true;
      return this;
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 5){
            printUsage();
            return;
        }
        String type = args[0];
        String exportFile = args[1];
        String brokerAddress = args[2];
        String plyqlAddress = args[3];
        String sql = args[4];

        DataExporter dataExporter = null;
        if (type.equals("file")) {
            dataExporter = DataExporter.local();
        } else if (type.equals("hdfs")) {
            dataExporter = DataExporter.hdfs();
        }
        if (dataExporter == null) {
            System.out.println("unknown export destination" + type);
            printUsage();
            return;
        }
        dataExporter.fromServer(brokerAddress)
                .usePylql(plyqlAddress)
                .withSQL(sql).toFile(exportFile);

        String exportType = "csv";
        if (args.length > 5){
            exportType = args[5];
        }
        if ("csv".equals(exportType)) {
            dataExporter.inCSVFormat();
        } else if ("tsv".equals(exportType)) {
            dataExporter.inTSVFormat();
        } else if ("hive".equals(exportType)) {
            dataExporter.inHiveFormat();
        } else {
            System.out.println("unknown export type" + exportType);
            printUsage();
            return;
        }
        dataExporter.export();
    }

    private static void printUsage(){
        System.out.println("Usage: DataExporter file|hdfs export_file broker_address plyql_address sql [export_type(hive|csv|tsv)]");
    }
}
