package io.druid.hyper.client;

import io.druid.hyper.client.exports.DataExporter;
import io.druid.hyper.client.exports.vo.ScanQuery;
import org.joda.time.DateTime;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class DataExporterTest {

    private static final String SERVER = "192.168.0.211:8086";
    private static final String LOCAL_FILE = "/tmp/wuxianjiRT.csv";
    private static final String REMOTE_FILE = "/hadoop/data1/wuxianjiRT.txt";
    private static final String DATA_SOURCE = "janpy-1";
    private static final List<String> COLUMNS = Arrays.asList("is_active", "event_id", "app_id", "is_installed", "HEAD_IP", "HEAD_ARGS");
    private static final int COUNT = 500000;

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
                .exportFromRS();

        query.setLimit(COUNT);

        DataExporter.local()
            .fromServer("192.168.0.211:8082")
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

//    curl -XGET http://192.168.0.211:8086/druid/hmaster/v1/datasources/segments/janpy-1
//	[{
//		"partition": 0,
//		"version": "0",
//		"interval": "1000-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z",
//		"servers": ["192.168.0.211:8087",
//		"192.168.0.212:8087"]
//	},
//	{
//		"partition": 1,
//		"version": "0",
//		"interval": "1000-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z",
//		"servers": ["192.168.0.212:8087",
//		"192.168.0.211:8087"]
//	},
//	{
//		"partition": 2,
//		"version": "0",
//		"interval": "1000-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z",
//		"servers": ["192.168.0.211:8087",
//		"192.168.0.212:8087"]
//	},
//	{
//		"partition": 3,
//		"version": "0",
//		"interval": "1000-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z",
//		"servers": ["192.168.0.211:8087",
//		"192.168.0.212:8087"]
//	}]
    public static void main(String[] args) throws Exception {
        System.out.println(new DateTime() + " start exporting data... ");
        for (int i = 0; i < 1; i++) {
            File file = new File(LOCAL_FILE);
            if (file.exists()) {
                System.out.println(String.format("delete file:%s:%,d", file, file.length()));
                file.delete();
            }
            DataExporterTest exporterTest = new DataExporterTest();
            exporterTest.exportToLocal();
            System.out.println(new DateTime() + "  " + i);
//            Thread.sleep(3000);
        }
        System.out.println(new DateTime() + " export successfully");
    }
}
