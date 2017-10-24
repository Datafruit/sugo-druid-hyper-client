package io.druid.hyper.client.exports;

import com.google.common.base.Preconditions;
import io.druid.hyper.client.exports.vo.ScanQuery;
import io.druid.hyper.client.util.JsonObjectIterator;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class DataExporter implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DataExporter.class);
    private static final MediaType DEFAULT_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final String SERVER_SCHEMA = "http://%s/druid/v2?pretty";

    private static final String SEPARATOR_COMMA = ",";
    private static final String SEPARATOR_TAB = "\t";
    private static final String SEPARATOR_HIVE = "\001";

    String filePath;
    String server;
    String separator;
    ScanQuery query;

    public static DataExporter local() {
        return new LocalDataExporter();
    }

    public static DataExporter hdfs() {
        return new HdfsDataExporter();
    }

    public void export() throws Exception {
        Preconditions.checkNotNull(server, "server can not be null.");
        Preconditions.checkNotNull(filePath, "export file can not be null.");
        Preconditions.checkNotNull(query, "query can not be null.");

        log.info("Start to export data from server [" + server + " ] to file [" + filePath + "].");
        long start = System.currentTimeMillis();

        init(filePath);

        OkHttpClient client = (new OkHttpClient.Builder())
                .connectTimeout(1800L, TimeUnit.SECONDS)
                .readTimeout(1800L, TimeUnit.SECONDS).build();
        RequestBody body = RequestBody.create(DEFAULT_MEDIA_TYPE, query.toString());
        Request request = (new Request.Builder()).url(server).post(body).build();
        Response response = client.newCall(request).execute();
        InputStream in = response.body().byteStream();

        try {
            JsonObjectIterator iterator = new JsonObjectIterator(in);
            while (iterator.hasNext()) {
                HashMap resultValue = iterator.next();
                if (resultValue != null) {
                    List<List<Object>> events = (List<List<Object>>) resultValue.get("events");
                    for (List<Object> event : events) {
                        writeRow(toLine(event));
                    }
                    flush();
                }
            }
        } catch (Exception e) {
            log.error("Write data to file error: " + e);
            throw e;
        } finally {
            try {
                close();
            } catch (IOException e) {
                log.error("Close resource error: " + e);
            }
        }

        long end = System.currentTimeMillis();
        log.info("Export data successfully, cost [" + (end-start) + "] million seconds.");
    }

    protected abstract void init(String filePath) throws IOException;

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
        this.server = String.format(SERVER_SCHEMA, server);
        return this;
    }

    public DataExporter toFile(String file) {
        this.filePath = file;
        return this;
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
}
