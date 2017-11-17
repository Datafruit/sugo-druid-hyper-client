package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataExporterTest {

    private static final String SERVER = "192.168.0.215:8082";
    private static final String LOCAL_FILE = "F:\\fxj_test.txt";
    private static final String REMOTE_FILE = "/hadoop/data1/wuxianjiRT.txt";
    private static final String DATA_SOURCE = "fxj_test";
    private static final List<String> COLUMNS = Arrays.asList("app_id");
    private static final int COUNT = 100000000;

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

    public void  exportToLocalUseSQL() throws Exception {
        DataExporter.local()
                .fromServer(SERVER)
                .toFile("D:\\fxj_test.csv")
                .inCSVFormat()
                .withSQL("select id,name,class from fxj_test")
                .usePylql("192.168.0.215:8001")
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
//        exporterTest.exportToLocalUseSQL();
//        exporterTest.exportToHdfs();
    }
}
