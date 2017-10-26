package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataExporterTest {

    private static final String SERVER = "192.168.0.212:8082";
    private static final String LOCAL_FILE = "E:\\data2\\data3\\wuxianjiRT.txt";
    private static final String REMOTE_FILE = "/hadoop/data1/wuxianjiRT.txt";
    private static final String DATA_SOURCE = "wuxianjiRT";
    private static final List<String> COLUMNS = Arrays.asList("SessionID", "Source", "Referrer", "Operator", "OsScreen", "Media", "IP", "UserID", "EventScreen", "Nation");
    private static final int COUNT = 1000;

    public void  exportToLocal() throws Exception {
        ScanQuery query = ScanQuery.builder()
                .select(COLUMNS)
                .from(DATA_SOURCE)
                .limit(COUNT)
                .build();

        DataExporter.local()
                .fromServer(SERVER)
                .toFile(LOCAL_FILE)
                .inCSVFormat()
                .withQuery(query)
                .export();
    }

    public void  exportToHdfs() throws Exception {
        ScanQuery query = ScanQuery.builder()
                .select(COLUMNS)
                .from(DATA_SOURCE)
                .limit(COUNT)
                .build();

        DataExporter.hdfs()
                .fromServer(SERVER)
                .toFile(REMOTE_FILE)
                .inHiveFormat()
                .withQuery(query)
                .export();
    }

    public static void main(String[] args) throws Exception {
        DataExporterTest exporterTest = new DataExporterTest();
        exporterTest.exportToLocal();
//        exporterTest.exportToHdfs();
    }
}
