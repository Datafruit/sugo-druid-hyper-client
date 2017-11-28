package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataExporterTest {

    private static final String SERVER = "192.168.0.212:8082";
    private static final String LOCAL_FILE = "D:\\wuxianjiRT.csv";
    private static final String REMOTE_FILE = "/hadoop/data1/wuxianjiRT.txt";
    private static final String DATA_SOURCE = "wuxianjiRT";
    private static final List<String> COLUMNS = Arrays.asList("Nation", "Operator", "ClientDeviceBrand", "SystemVersion", "EventScreen", "Network", "EventAction", "ClientDeviceModel");
    private static final int COUNT = 16;

    public void  exportToLocal() throws Exception {
        ScanQuery query = ScanQuery.builder()
                .select(COLUMNS)
                .from(DATA_SOURCE)
//                .limit(COUNT)
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
                .withSQL("select id,name,class from fxj_test limit 16")
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
