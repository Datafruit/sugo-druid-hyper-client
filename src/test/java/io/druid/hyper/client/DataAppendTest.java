package io.druid.hyper.client;

import com.google.common.collect.Lists;
import io.druid.hyper.client.imports.DataSender;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataAppendTest {

  private static final String HMASTER = "192.168.0.211:8086";
  private static final String DATA_SOURCE = "multi-1";

  private DataSender sender;

  private DataSender getSender() {
    if (sender == null) {
      sender = DataSender.builder().toServer(HMASTER).ofDataSource(DATA_SOURCE).build();
    }
    return sender;
  }

  private void addBatch() throws Exception {
    //        app_id,event,HEAD_ARGS,age,count,score,avg
    Random r = new Random();
    DataSender sender = getSender();
    List columns = Lists.newArrayList("app_id", "event", "HEAD_ARGS", "age", "count", "score", "avg");
    long s1 = System.currentTimeMillis();

    long min = Integer.MAX_VALUE;
    long max = Integer.MIN_VALUE;
    int base = 10000000;
    for (int i = 0; i < base; i++) {
//      sender.add(Lists.newArrayList(String.format("id-%07d", i), "event" + r.nextInt(10),
//          "args-" + i, r.nextInt(60) + 10, 100 + r.nextInt(600), 100 * r.nextFloat(), 1000 * r.nextDouble()));
    }
    long s2 = System.currentTimeMillis() + 4;
    System.out.println(String.format("add spend time:%,d", s2 - s1));

    for (int i = 0; i < base; i++) {
//      sender.update(columns, Lists.newArrayList(String.format("id-%07d", i), "event" + r.nextInt(10),
//          "args-" + r.nextInt(60), r.nextInt(60) + 10, 100 + r.nextInt(600), 100 * r.nextFloat(), 1000 * r.nextDouble()),
//          Lists.newArrayList(false, true, true, true, true, true, true));
//      sender.update(columns, Lists.newArrayList(String.format("id-%07d", i), "event" + r.nextInt(10),
//          "args-" + r.nextInt(60), r.nextInt(60) + 10, 100 + r.nextInt(600), 100 * r.nextFloat(), 1000 * r.nextDouble()));
      Map<String, Object> row = new HashMap<>();
      row.put("app_id", String.format("id-%07d", i));
      row.put("event", "event" + r.nextInt(10));
      row.put("HEAD_ARGS",  "args-" + r.nextInt(60));
      row.put("age", r.nextInt(60) + 10);
      row.put("count", 100 + r.nextInt(600));
      row.put("score", 100 * r.nextFloat());
      row.put("avg", 1000 * r.nextDouble());
//      sender.update(row);

      Map<String, Boolean> flagMap = new HashMap<>();
      flagMap.put("app_id", false);
      flagMap.put("event", true);
      flagMap.put("HEAD_ARGS",  true);
      flagMap.put("age", true);
      flagMap.put("count", true);
      flagMap.put("score", true);
      flagMap.put("avg", true);

      sender.update(row, flagMap);
    }

    if (s2 - s1 > 0) {
      return;
    }

    long spend = 0;
    long total = 0;

    s1 = System.currentTimeMillis();
    long last = s1;
    long now = s1;
    int idx = 1;
    for (int i = 1; i < 5000000; i++) {
      String values = "";
      int len = r.nextInt(6);
      for (int n = 0; n < len; n++) {
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
            new DateTime(), i, total, max, min, total * 1 / idx++, spend));
        last = now;
      }
    }
    s2 = System.currentTimeMillis();
    System.out.println(String.format("update spend time:%,d", s2 - s1));
  }

  public static void main(String[] args) throws Exception {
    DataAppendTest senderTest = new DataAppendTest();
    senderTest.addBatch();
    senderTest.close();
  }

  private void close() throws IOException {
    sender.close();
    sender = null;
  }
}
