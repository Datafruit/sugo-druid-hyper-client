package io.druid.hyper.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.hyper.client.imports.DataSender;

import java.util.List;
import java.util.Map;

public class DataSenderTest {

    private static final String HMASTER = "192.168.0.215:8086";
    private static final String DATA_SOURCE = "huangama3";

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
        senderTest.add();
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

}
