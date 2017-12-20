package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ExporterAppendDataTest {

    private static final String SERVER = "192.168.0.211:8086";
    private static final String LOCAL_FILE = "/tmp/wuxianjiRT.csv";
    private static final String DATA_SOURCE = "multi-1";
    private static final List<String> COLUMNS = Arrays.asList("app_id", "event", "HEAD_ARGS", "age", "count", "score", "avg");
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
    }

    public static void main(String[] args) throws Exception {
        System.out.println(new DateTime() + " start exporting data... ");
        Random random = new Random();
        int rowCount;
        for (int i = 0; i < 1; i++) {
            File file = new File(LOCAL_FILE);
            if (file.exists()) {
                System.out.println(String.format("delete file:%s:%,d, row:%,d", file, file.length(), getFileRowCount(file)));
                file.delete();
            }
            rowCount = random.nextInt(2000000);
            ExporterAppendDataTest exporterTest = new ExporterAppendDataTest();
            exporterTest.exportToLocal(rowCount);
            System.out.println(String.format("%d --- %s expect row:%,d, exported data row:%,d",
                i, new DateTime(), rowCount, getFileRowCount(file)));
            Thread.sleep(3000);
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
