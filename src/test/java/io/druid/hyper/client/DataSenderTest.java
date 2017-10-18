package io.druid.hyper.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.hyper.client.imports.DataSender;

import java.util.List;
import java.util.Map;

public class DataSenderTest {

    private static final String HMASTER = "hd-node-228-28.meizu.gz:8086";
    private static final String DATA_SOURCE = "igola_profile";

    public void add() throws Exception {
        DataSender sender = getSender();
//        sender.add("1001|M|18");
//        sender.add(Lists.newArrayList("1002", "F", "16"));
//        sender.add(Lists.newArrayList("1003", "F", "24"));
        sender.close();
    }

    public void update() throws Exception {
        DataSender sender = getSender();
        Map dataMap = Maps.newHashMap();
//        dataMap.put("uid", "1001");
//        dataMap.put("profile_age", "99");
//        sender.update(dataMap);

        List columns = Lists.newArrayList("uid", "new_class");
//        sender.update(columns, Lists.newArrayList("1002", "snow"));
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
//        senderTest.add();
        senderTest.update();
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
