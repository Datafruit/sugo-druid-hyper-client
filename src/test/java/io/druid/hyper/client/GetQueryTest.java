package io.druid.hyper.client;

import com.google.common.collect.Lists;
import io.druid.hyper.client.query.GetQueryClient;

import java.util.List;
import java.util.Map;

public class GetQueryTest {
    private static final String HMASTER = "192.168.0.215:8086";
    private static final String DATA_SOURCE = "fxj_test";

    public void query() throws Exception {
        Map<String, Object> res =  new GetQueryClient(HMASTER, DATA_SOURCE).getRow("8846512");
        System.out.println(res);
    }

    public void query(List<String> columns) throws Exception {
        Map<String, Object> res =  new GetQueryClient(HMASTER, DATA_SOURCE).getRow("8846512", columns);
        System.out.println(res);
    }

    public static void main(String[] args) throws Exception {
        GetQueryTest getQueryTest = new GetQueryTest();
        getQueryTest.query(Lists.newArrayList("app_id", "is_active"));
        getQueryTest.query();
    }
}
