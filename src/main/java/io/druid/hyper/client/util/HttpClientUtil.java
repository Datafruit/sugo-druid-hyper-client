package io.druid.hyper.client.util;

import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpClientUtil {

    private static final long DEFAULT_TIME_OUT = 30 * 60; // 30 minutes
    private static final MediaType DEFAULT_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    private static final OkHttpClient HTTP_CLIENT = new OkHttpClient.Builder()
            .connectTimeout(DEFAULT_TIME_OUT, TimeUnit.SECONDS)
            .readTimeout(DEFAULT_TIME_OUT, TimeUnit.SECONDS)
            .build();

    public static String get(String url) throws IOException {
        Request request = new Request.Builder().url(url).build();

        Response response = HTTP_CLIENT.newCall(request).execute();
        String result = response.body().string();

        return result;
    }

    public static String post(String url, String jsonData) throws Exception {
        RequestBody body = RequestBody.create(DEFAULT_MEDIA_TYPE, jsonData);
        Request request = new Request.Builder().url(url).post(body).build();

        Response response = HTTP_CLIENT.newCall(request).execute();
        int rtnCode = response.code();
        String result = response.body().string();

        if (rtnCode != 200) {
            throw new Exception("Post data failed. error:" + result);
        }

        return result;
    }
}

