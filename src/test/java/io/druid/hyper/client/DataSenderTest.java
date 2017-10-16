package io.druid.hyper.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.hyper.client.imports.DataSender;

import java.util.List;
import java.util.Map;

public class DataSenderTest {

    private static final String HMASTER = "192.168.0.217:8086";
    private static final String DATA_SOURCE = "huangama";

    public void add() throws Exception {
        DataSender sender = getSender();
        sender.add("1\001sugo\0011001\0011\001192.168.0.01\001[\"haha\", \"hehe\"]");
        sender.add(Lists.newArrayList("2", "sugo2", "1002", "0", "192.168.0.66", "[\"wawa\"]"));
        sender.add(Lists.newArrayList("3", "sugo3", "1003", "1", "192.168.0.88", "[\"gaga\"]"));
        sender.close();
    }

    public void update() throws Exception {
        DataSender sender = getSender();
        Map dataMap = Maps.newHashMap();
        dataMap.put("app_id", "1001");
        dataMap.put("event_id", "7878");
        sender.update(dataMap);

        List columns = Lists.newArrayList("app_id", "event_id");
        sender.update(columns, Lists.newArrayList("1002", "9696"));
        sender.update(columns, Lists.newArrayList("1003", "4455"));
        sender.close();
    }

    private DataSender getSender() {
        return DataSender.builder().toServer(HMASTER).ofDataSource(DATA_SOURCE).build();
    }

    public static void main(String[] args) throws Exception {
        DataSenderTest senderTest = new DataSenderTest();
        senderTest.add();
//        senderTest.update();
    }

    public static void main1(String[] args) throws Exception {
        String delimiter = "\001";
        String delimiter2 = "\u0001";
        List list = Lists.newArrayList("2", "sugo2", "1002", "0", "192.168.0.66", "[\"wawa\"]");
        String str = Joiner.on(delimiter).join(list);
        System.out.println(str);

        List<String> list2 = Splitter.on(delimiter2).splitToList(str);
        System.out.println(list2);
    }

}
