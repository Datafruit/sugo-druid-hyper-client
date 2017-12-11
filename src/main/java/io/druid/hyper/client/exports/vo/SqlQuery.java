package io.druid.hyper.client.exports.vo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.hyper.client.util.HttpClientUtil;

import java.util.Map;

public class SqlQuery implements Query {

    private static final String PLYQL_SCHEMA = "http://%s/get-query";

    private String plyql;
    private Map<String, Object> queryMap;

    public SqlQuery(String plyql, String sql) throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(plyql), "Must specify plyql address with method 'usePylql' for parsing SQL.");
        this.plyql = plyql;
        this.queryMap = parseSQL(sql);
    }

    @Override
    public String getDataSource() {
        return queryMap.get("dataSource").toString();
    }

    @Override
    public int getLimit() {
        return (int) queryMap.get("limit");
    }

    @Override
    public void setLimit(int limit) {
        queryMap.put("limit", limit);
    }

    @Override
    public void setIntervals(Object intervals) {
        queryMap.put("intervals", intervals);
    }

    @Override
    public String queryString() throws Exception {
        return jsonMapper.writeValueAsString(queryMap);
    }

    private Map<String, Object> parseSQL(String sql) throws Exception {
        String plyqlQueryUrl = String.format(PLYQL_SCHEMA, plyql);
        String response = HttpClientUtil.post(plyqlQueryUrl, String.format("{\"sql\":\"%s\",\"scanQuery\":true,\"hasLimit\":true}", sql));
        Map<String, Object> resMap = jsonMapper.readValue(response, Map.class);
        Map<String, Object> result = (Map<String, Object>) resMap.get("result");
        if (resMap == null || result == null) {
            throw new RuntimeException("Parse sql error: " + response + ".\n Please check your plyql [" + plyql + "] is correct? " +
                    "\n Or your sql [" + sql + "] grammar is correct?");
        }

        Map<String, Object> context = (Map<String, Object>) result.get("context");
        context.put("timeout", DEFAULT_TIMEOUT);

        if (result.get("limit") == null) {
            result.put("limit", DEFAULT_LIMIT);
        }

        return result;
    }
}
