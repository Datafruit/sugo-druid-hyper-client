package io.druid.hyper.client;

import com.google.common.collect.Lists;

import java.util.List;

public class IGolaDataWithNull {

    // 1~10
    public static final List<String> PROFILE_GENDER = Lists.newArrayList("男", "女", "未知");
    public static final List<Integer> PROFILE_AGE = Lists.newArrayList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,null,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,null,34,35,36,37,38,39,40,41,null,43,44,null,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,null,87,88,89,90,91,92,93,94,95,96,97,98,99,100);
    public static final List<Integer> PROFILE_REG_BP = Lists.newArrayList(1,2,3,4,5,6,7,8,9,null,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,null,27,28,29,30,31,32,33,34,35,36,37,38,39,40,null,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,null,60,61,62,63,64,65,66,null,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100);
    public static final List<Integer> PROFILE_LOGIN_BP = Lists.newArrayList(1,2,3,4,5,null,7,8,9,10,11,12,13,14,null,16,17,18,19,20,21,22,23,24,null,26,27,28,29,30,31,32,null,34,35,36,37,38,39,40,null,42,43,44,45,46,47,48,49,50,51,52,null,54,55,56,57,58,59,60,61,62,63,64,65,66,null,68,69,70,71,72,73,74,75,76,77,78,79,80,null,82,83,84,85,86,87,88,null,90,91,92,93,94,95,96,97,98,99,100);
    public static final List<String> VALUE_FARECLASS = Lists.newArrayList("经济舱", null, "头等舱");
    public static final List<Integer> VALUE_ORDER_BP = Lists.newArrayList(1,2,3,4,5,6,7,8,null,10,11,12,13,14,15,16,null,18,19,20,null,22,23,24,25,26,27,null,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,null,54,55,56,57,58,59,60,61,62,63,64,null,66,67,68,69,70,71,72,null,74,75,76,77,78,79,80,81,82,83,84,85,null,87,88,89,90,91,92,93,94,95,96,97,98,99,100);
    public static final List<Double> VALUE_U_PRICE_AVG = Lists.newArrayList(100d,200d,null,400d,500d,600d,700d,null,900d,1000d,1200d,1500d,null,2000d,2300d,2500d,2700d,3000d,3200d,3600d,3900d,4000d,4500d,4800d,5000d,5500d,6000d,6600d,6800d,null,7200d,7500d,8000d,9000d,10000d,12000d,15000d,18000d,20000d,25000d,null,40000d,50000d);
    public static final List<Double> VALUE_T_PRICE_AVG = Lists.newArrayList(100d,200d,300d,null,500d,600d,700d,null,900d,1000d,null,1500d,1800d,null,2300d,2500d,2700d,3000d,3200d,3600d,3900d,4000d,null,4800d,5000d,5500d,6000d,6600d,6800d,null,7200d,7500d,8000d,9000d,10000d,12000d,15000d);
    public static final List<Integer> VALUE_ORDER_NUM = Lists.newArrayList(1,2,3,4,5,6,7,8,9,null,11,12,13,14,15,16,null,18,19,20,null,22,23,24,25,26,null,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,null,46,47,48,49,50,51,52,53,null,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,null,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100);
    public static final List<Integer> VALUE_ORDER_COUPON_NUM = Lists.newArrayList(1,2,3,4,5,6,null,8,9,10,11,null,13,14,15,16,17,18,null,20,21,22,23,null,25,26,27,28,29,30,31,null,33,34,35,36,37,38,39,null,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,null,66,67,68,null,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,null,88,89,90,91,92,93,94,95,96,97,98,99,100);
    // 11~20
    public static final List<Double> VALUE_MONEY = Lists.newArrayList(null,1200d,1500d,null,2000d,2300d,2500d,null,3000d,3200d,3600d,3900d,4000d,4500d,4800d,5000d,5500d,6000d,6600d,null,7000d,7200d,7500d,8000d,null,10000d,12000d,15000d,20000d,30000d,40000d,50000d,60000d,70000d,80000d,90000d,100000d);
    public static final List<Double> VALUE_MONEY_COUPON = Lists.newArrayList(100d,null,300d,400d,null,600d,700d,800d,900d,1000d,null,1500d,1800d,2000d);
    public static final List<Integer> BEHAVIOR_TICKET_DOMESTIC_BP = Lists.newArrayList(1,2,3,4,5,6,7,8,9,null,12,15,20,25,30,35,40,45,null,55,60,70,80,90,null);
    public static final List<Integer> BEHAVIOR_TICKET_INTER_BP = Lists.newArrayList(1,2,3,4,5,6,null,8,9,10,12,15,20,25,30,null,40,45,50,null,60,70,80,90,100);
    public static final List<Integer> BEHAVIOR_SEARCH_TIME = Lists.newArrayList(1,2,3,4,5,6,null,8,9,10,11,null,13,14,15,16,17,null,19,20,21,22,23,24,25,26,27,28,29,30,31);
    public static final List<String> BEHAVIOR_SEARCH_DST_TOP1 = Lists.newArrayList(null, "美国", "英国", "法国", "意大利", "德国");
    public static final List<String> BEHAVIOR_SEARCH_DST_TOP2 = Lists.newArrayList("澳洲", "新加坡", "马来西亚", "韩国", null, null);
    public static final List<String> BEHAVIOR_SEARCH_DST_TOP3 = Lists.newArrayList("中国", null, "古巴", "伊朗", null, "巴基斯坦");
    public static final List<Integer> TICKET_SEARCH = Lists.newArrayList(1,2,3,null,5,6,7,8,9,null,11,12,null,14,15,16,17,18,19,20,null,22,23,24,25,null,27,28,29,30,35,40,50);
    public static final List<Integer> TICKET_ORDER = Lists.newArrayList(1,2,3,null,5,6,null,8,9,null,11,12,13,null,15,16,null,18,19,20);
    // 21~30
    public static final List<Integer> TICKET_CHANGE = Lists.newArrayList(1,2,null,4,5,6,7,8,9,null,11,12,13,null,15);
    public static final List<String> LOCATION_CITY = Lists.newArrayList("广州", null, "深圳");
    public static final List<String> LOCATION_FROM_TOP1 = Lists.newArrayList("广州");
    public static final List<String> LOCATION_FROM_TOP2 = Lists.newArrayList("北京");
    public static final List<String> LOCATION_FROM_TOP3 = Lists.newArrayList("深圳");
    public static final List<String> LOCATION_TO_TOP1 = Lists.newArrayList("新加坡", "巴塞罗那", null);
    public static final List<String> LOCATION_TO_TOP2 = Lists.newArrayList("洛杉矶", "西雅图", null);
    public static final List<String> LOCATION_TO_TOP3 = Lists.newArrayList("巴塞罗那", "马德里", "巴黎");
    public static final List<String> LOCATION_AIRLINE_TOP1 = Lists.newArrayList("广州-新加坡", "广州-巴塞罗那", null);
    public static final List<String> LOCATION_AIRLINE_TOP2 = Lists.newArrayList("北京-洛杉矶", "北京-西雅图", null);
    // 31~40
    public static final List<String> LOCATION_AIRLINE_TOP3 = Lists.newArrayList("深圳-巴塞罗那", null, "深圳-巴黎");
    public static final List<String> HOLIDAY_YUANDAN = Lists.newArrayList("0", "1");
    public static final List<String> HOLIDAY_CHUNJIE = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_HANJIA = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_QINGMING = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_WUYI = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_DUANWU = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_SHUJIA = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_ZHONGQIU = HOLIDAY_YUANDAN;
    public static final List<String> HOLIDAY_GUOQIN = HOLIDAY_YUANDAN;
    // 41~43
    public static final List<String> HOLIDAY_SHENGDAN = HOLIDAY_YUANDAN;
    public static final List<String> AIRLINE_COMPANY = Lists.newArrayList("南航", "日本航空", "美国航空", null, "国航");
    public static final List<String> AIRLINE_AIRCRAFT = Lists.newArrayList("波音737", null, "波音777", "波音787");
}