package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataExporterTest {

    private static final String SERVER = "192.168.0.211:8082";
    private static final String LOCAL_FILE = "/tmp/wuxianjiRT.txt";
    private static final String REMOTE_FILE = "/hadoop/data1/wuxianjiRT.txt";
    private static final String DATA_SOURCE = "janpy-1";
    private static final List<String> COLUMNS = Arrays.asList("is_active", "event_id", "app_id", "is_installed", "HEAD_IP", "HEAD_ARGS");
    private static final int COUNT = 100000000;

    public void  exportToLocal() throws Exception {
        ScanQuery query = ScanQuery.builder()
                .select(COLUMNS)
                .from(DATA_SOURCE)
                .limit(COUNT)
//                .where(
//                    ScanQuery.dimension("Operator").equal("电信"),
//                    ScanQuery.or(
//                        ScanQuery.dimension("IP").in("222.71.50.44", "139.196.231.205"),
//                        ScanQuery.dimension("EventScreen").notIn("日程表")
//                    )
//                )
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
        for(int i = 0; i < 1000; i++) {
            File file = new File(LOCAL_FILE);
            if(file.exists()) {
                System.out.println(String.format("%,d", file.length()));
                file.delete();
            }
            DataExporterTest exporterTest = new DataExporterTest();
            exporterTest.exportToLocal();
            System.out.println(new DateTime() +"  " + i);
            Thread.sleep(3000);
        }
    }
}
