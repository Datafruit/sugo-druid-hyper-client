package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;
import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Method.get;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class DataExporterTest {

    private static final String SERVER = "192.168.0.225:8086";
    private static final String LOCAL_FILE = "/tmp/wuxianjiRT.csv";
    private static final String REMOTE_FILE = "/hadoop/data1/wuxianjiRT.txt";
    private static final String DATA_SOURCE = "janpy-1";
    private static final List<String> COLUMNS = Arrays.asList("is_active", "event_id", "app_id", "is_installed", "HEAD_IP", "HEAD_ARGS");
    private static final int COUNT = 500000;

    public void  exportToLocal(int rowCount) throws Exception {
        ScanQuery query = ScanQuery.builder()
                .select(COLUMNS)
                .from(DATA_SOURCE)
                .limit(rowCount)
                .build();

        DataExporter.local()
                .fromServer(SERVER)
                .toFile(LOCAL_FILE)
                .inCSVFormat()
                .progressLog()
                .withQuery(query)
                .export();

//        query.setLimit(COUNT);
//
//        DataExporter.local()
//            .fromServer("192.168.0.211:8082")
//            .toFile(LOCAL_FILE)
//            .inCSVFormat()
//            .withQuery(query)
//            .export();
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

//    curl -XGET http://192.168.0.211:8086/druid/hmaster/v1/datasources/segments/janpy-1
    public static void main(String[] args) throws Exception {
        System.out.println(new DateTime() + " start exporting data... ");
        Random random = new Random();
        int rowCount;
        for (int i = 0; i < 100; i++) {
            File file = new File(LOCAL_FILE);
            if (file.exists()) {
                System.out.println(String.format("delete file:%s:%,d, row:%,d", file, file.length(), getFileRowCount(file)));
                file.delete();
            }
            rowCount = random.nextInt(2000000);
            DataExporterTest exporterTest = new DataExporterTest();
            rowCount = 30000000;
//            rowCount = 100;
            long s1 = System.currentTimeMillis();
            exporterTest.exportToLocal(rowCount);
            long s2 = System.currentTimeMillis();
            System.out.println(String.format("%d --- spend: %,d--- %s expect row:%,d, exported data row:%,d",
                i, s2-s1, new DateTime(), rowCount, getFileRowCount(file)));
//            Thread.sleep(3000);
            file = new File(LOCAL_FILE);
            if (file.exists()) {
                System.out.println(String.format("delete file:%s:%,d, row:%,d", file, file.length(), getFileRowCount(file)));
                file.delete();
            }
            System.out.println("\n\n\n\n");
        }
        System.out.println(new DateTime() + " export successfully");
    }

    private static int getFileRowCount(File file) {
        long fileLength = file.length();
        LineNumberReader rf = null;
        int lines = 0;
        try {
            rf = new LineNumberReader(new FileReader(file));
            if (rf != null) {
                rf.skip(fileLength);
                lines = rf.getLineNumber();
                rf.close();
            }
        } catch (IOException e) {
            if (rf != null) {
                try {
                    rf.close();
                } catch (IOException ee) {
                }
            }
        }
        return lines;
    }
}
