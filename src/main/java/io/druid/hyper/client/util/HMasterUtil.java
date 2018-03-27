package io.druid.hyper.client.util;

import org.apache.commons.lang.StringUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HMasterUtil {

  private static final String QUERY_LEADER_SCHEMA = "http://%s/druid/hmaster/v1/leader";
  private static final Logger log = LoggerFactory.getLogger(HMasterUtil.class);
  public static String getLeader(String[] hmasters){
    String serverStr = null;
    for (String hmaster : hmasters) {
      String queryUrl = String.format(QUERY_LEADER_SCHEMA, hmaster);
      try {
        serverStr = HttpClientUtil.get(queryUrl);
        queryUrl = String.format(QUERY_LEADER_SCHEMA, serverStr);
        //make sure response result is equals "hmaster:port"
        if (HttpClientUtil.get(queryUrl).equals(serverStr))
          return serverStr;
      } catch (IOException e) {
        log.warn(String.format("query %s failed", hmaster), e);
        continue;
      }
    }
    if (StringUtils.isBlank(serverStr)){
      log.error("hmaster not found !!!");
      return null;
    }
    return serverStr;
  }
}
