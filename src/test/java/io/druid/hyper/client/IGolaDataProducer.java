package io.druid.hyper.client;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.druid.hyper.client.imports.DataSender;

import java.io.*;
import java.util.List;
import java.util.Random;

public class IGolaDataProducer {

    private static final String HMASTER = "192.168.0.217:8086";
    private static final String DATA_SOURCE = "igola_profile";
    private static final String DATA_SOURCE_TAG = "igola_profile_tag";
    private static final String UID_FORMAT = "A%05d";
    private static final int TOTAL_ROWS = 50000;

    public void add() throws Exception {
        DataSender sender = getSender();
        for (int i = 1; i <= TOTAL_ROWS; i++) {
            sender.add(generateSingleRow(i));
            System.out.println("Send " + i + " successfully.");
        }
        sender.close();
    }

    public void addTag() throws Exception {
        DataSender sender = DataSender.builder().toServer(HMASTER).ofDataSource(DATA_SOURCE_TAG).build();
        List<List> dataList = readTagData();
        int i = 0;
        for (List data : dataList) {
            sender.add(data);
            System.out.println("Send " + i++ + " successfully.");
        }
        sender.close();
    }

    private String generateUID(int rowNum) {
        return String.format(UID_FORMAT, rowNum);
    }

    private List generateSingleRow(int rowNum) {
        return Lists.newArrayList(
                generateUID(rowNum),
                getRandomValue(IGolaDataWithNull.PROFILE_GENDER),
                getRandomValue(IGolaDataWithNull.PROFILE_AGE),
                getRandomValue(IGolaDataWithNull.PROFILE_REG_BP),
                getRandomValue(IGolaDataWithNull.PROFILE_LOGIN_BP),
                getRandomValue(IGolaDataWithNull.VALUE_FARECLASS),
                getRandomValue(IGolaDataWithNull.VALUE_ORDER_BP),
                getRandomValue(IGolaDataWithNull.VALUE_U_PRICE_AVG),
                getRandomValue(IGolaDataWithNull.VALUE_T_PRICE_AVG),
                getRandomValue(IGolaDataWithNull.VALUE_ORDER_NUM),
                getRandomValue(IGolaDataWithNull.VALUE_ORDER_COUPON_NUM),
                getRandomValue(IGolaDataWithNull.VALUE_MONEY),
                getRandomValue(IGolaDataWithNull.VALUE_MONEY_COUPON),
                getRandomValue(IGolaDataWithNull.BEHAVIOR_TICKET_DOMESTIC_BP),
                getRandomValue(IGolaDataWithNull.BEHAVIOR_TICKET_INTER_BP),
                getRandomValue(IGolaDataWithNull.BEHAVIOR_SEARCH_TIME),
                getRandomValue(IGolaDataWithNull.BEHAVIOR_SEARCH_DST_TOP1),
                getRandomValue(IGolaDataWithNull.BEHAVIOR_SEARCH_DST_TOP2),
                getRandomValue(IGolaDataWithNull.BEHAVIOR_SEARCH_DST_TOP3),
                getRandomValue(IGolaDataWithNull.TICKET_SEARCH),
                getRandomValue(IGolaDataWithNull.TICKET_ORDER),
                getRandomValue(IGolaDataWithNull.TICKET_CHANGE),
                getRandomValue(IGolaDataWithNull.LOCATION_CITY),
                getRandomValue(IGolaDataWithNull.LOCATION_FROM_TOP1),
                getRandomValue(IGolaDataWithNull.LOCATION_FROM_TOP2),
                getRandomValue(IGolaDataWithNull.LOCATION_FROM_TOP3),
                getRandomValue(IGolaDataWithNull.LOCATION_TO_TOP1),
                getRandomValue(IGolaDataWithNull.LOCATION_TO_TOP2),
                getRandomValue(IGolaDataWithNull.LOCATION_TO_TOP3),
                getRandomValue(IGolaDataWithNull.LOCATION_AIRLINE_TOP1),
                getRandomValue(IGolaDataWithNull.LOCATION_AIRLINE_TOP2),
                getRandomValue(IGolaDataWithNull.LOCATION_AIRLINE_TOP3),
                getRandomValue(IGolaDataWithNull.HOLIDAY_YUANDAN),
                getRandomValue(IGolaDataWithNull.HOLIDAY_CHUNJIE),
                getRandomValue(IGolaDataWithNull.HOLIDAY_HANJIA),
                getRandomValue(IGolaDataWithNull.HOLIDAY_QINGMING),
                getRandomValue(IGolaDataWithNull.HOLIDAY_WUYI),
                getRandomValue(IGolaDataWithNull.HOLIDAY_DUANWU),
                getRandomValue(IGolaDataWithNull.HOLIDAY_SHUJIA),
                getRandomValue(IGolaDataWithNull.HOLIDAY_ZHONGQIU),
                getRandomValue(IGolaDataWithNull.HOLIDAY_GUOQIN),
                getRandomValue(IGolaDataWithNull.HOLIDAY_SHENGDAN),
                getRandomValue(IGolaDataWithNull.AIRLINE_COMPANY),
                getRandomValue(IGolaDataWithNull.AIRLINE_AIRCRAFT)
               );
    }

    private Object getRandomValue(List valueList) {
        Random random = new Random();
        int hitIdx = random.nextInt(valueList.size());
        return valueList.get(hitIdx);
    }

    private DataSender getSender() {
        return DataSender.builder().toServer(HMASTER).ofDataSource(DATA_SOURCE).build();
    }

    private List readTagData() throws IOException {
        List dataList = Lists.newArrayList();
        File data = new File("F:\\igola_profile_tag_data.txt");
        BufferedReader reader = new BufferedReader(new FileReader(data));
        String line;
        while ((line = reader.readLine()) != null) {
            List singleList = Splitter.on("\t").splitToList(line);
            dataList.add(singleList);
        }

        return dataList;
    }

    public static void main(String[] args) throws Exception {
        IGolaDataProducer producer = new IGolaDataProducer();
        producer.add();
//        producer.addTag();
    }

}
