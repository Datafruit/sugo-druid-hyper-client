package io.druid.hyper.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.hyper.client.imports.DataSender;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataSenderTest {

    private static final String HMASTER = "192.168.0.211:8086";
    private static final String DATA_SOURCE = "janpy-1";

    public void add() throws Exception {
        DataSender sender = getSender();
        List<String> columns = Lists.newArrayList("name", "name2", "type", "tag_name", "tag_value");
        sender.add(Lists.newArrayList("1001", "Jack", "1", "tag1", "snow"));
        sender.add(Lists.newArrayList("1002", "Tom", "1", "tag1", "rain"));
        sender.add(Lists.newArrayList("1003", "Cruise", "0", "tag0", ""));
        sender.add(Lists.newArrayList("1004", "Peter", "0", "tag0", ""));
        sender.add(Lists.newArrayList("1005", "Zack", "0", "tag0", ""));
        sender.add(Lists.newArrayList("1006", "Julie", "1", "tag2", "storm"));
//        sender.add("1001|M|18");
//        sender.add(Lists.newArrayList("1002", "F", "16"));
//        sender.add(Lists.newArrayList("1003", "F", "24"));
//        sender.add(Lists.newArrayList("1001", "F", "like", "snow"));
//        sender.add(Lists.newArrayList("1002", "M", "like", "rain"));
        sender.close();
    }

    public void update() throws Exception {
        DataSender sender = getSender();
        Map dataMap = Maps.newHashMap();
        dataMap.put("app_id", "3");
        dataMap.put("is_installed", "fog");
        dataMap.put("HEAD_ARGS", "haha,hehe");
        sender.update(dataMap);

//        List columns = Lists.newArrayList("name", "new_class22");
//        sender.update(columns, Lists.newArrayList("1002", "hello22"));
//        sender.update(columns, Lists.newArrayList("1003", "rain"));
        sender.close();
    }

    public void delete() throws Exception {
        DataSender sender = getSender();
        sender.delete("1001");
        sender.close();
    }

    private DataSender getSender() {
        return DataSender.builder().toServer(HMASTER).ofDataSource(DATA_SOURCE).build();
    }

    public static void main(String[] args) throws Exception {
        DataSenderTest senderTest = new DataSenderTest();
        senderTest.addBatch();
//        senderTest.update();
//        senderTest.delete();
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

    private void addBatch() throws Exception {
        Random r = new Random();
        DataSender sender = getSender();
        List columns = Lists.newArrayList("is_active", "event_id", "app_id", "is_installed", "HEAD_IP", "HEAD_ARGS");
        long s1 = System.currentTimeMillis();

        long min = Integer.MAX_VALUE;
        long max = Integer.MIN_VALUE;
        int base = 1000000;
        for(int i =0; i < base;i++) {
            String values = "test1" + r.nextInt(100000000);
            sender.add(Lists.newArrayList("" + r.nextInt(3), "event" + r.nextInt(10), i, "" + r.nextInt(3), "192.168.0." + r.nextInt(256), values));
            if (i % 100000 == 0) {
                System.out.println(i);
            }
        }
        long s2 = System.currentTimeMillis();
        System.out.println(String.format("add spend time:%,d", s2 - s1));

        long spend = 0;
        long total = 0;

        s1 = System.currentTimeMillis();
        long last = s1;
        long now = s1;
        int idx = 1;
        for (int i = 1; i < 500000000; i++) {
            String values = "";
            int len = r.nextInt(6);
            for(int n = 0; n < len;n++) {
                values += "wawa-" + r.nextInt(100000000) + ",";
            }
            values += "test1" + r.nextInt(100000000);

            sender.update(columns, Lists.newArrayList("" + r.nextInt(3), "event" + r.nextInt(10), r.nextInt(base), "" + r.nextInt(3), "192.168.0." + r.nextInt(256), values));
            if (i % 100000 == 0) {
                now = System.currentTimeMillis();
                spend = now - last;
                total = now - s1;
                min = Math.min(min, spend);
                max = Math.max(max, spend);
                System.out.println(String.format("%s \t %,10d \t total time:%,8d \t max:%,6d \t min:%,6d \t avg:%,6d \t spend time:%,6d",
                    new DateTime(), i, total, max, min, total * 1/idx++, spend));
                last = now;
            }
        }
        s2 = System.currentTimeMillis();
        System.out.println(String.format("update spend time:%,d", s2 - s1));
        sender.close();
    }
}
