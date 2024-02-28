package com.example.demo.cep;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/11/17 17:19
 */
public class asdasds {

    public static void main(String[] args) throws IOException {


        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\r\n    \"dataType\": 1,\r\n    \"dateType\": 2,\r\n    \"merchantLabel\": \"0\",\r\n    \"startTime\": \"2023-12-15\",\r\n    \"endTime\": \"2023-12-15\",\r\n    \"isMerchantId\": false,\r\n    \"page\": 1,\r\n    \"pageSize\": 10,\r\n    \"rows\": 0,\r\n    \"offsetCount\": 0\r\n}");
        Request request = new Request.Builder()
                .url("https://pro-bigdata-report-web.sportxxxr1pub.com/api/panda-report/reportDate/merchantReportDataList")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .addHeader("Cookie", "incap_ses_428_2771936=t2h0RzSXdxRTd8ji4JDwBRFefWUAAAAAGNClggy1xXr3/PCuhnf+zg==; nlbi_2771936=4iaiDPmKHBl4VijCxXKW9AAAAACzIWZ600OZ8XSNDZu4aEQu; visid_incap_2771936=TBqRQPJ1Qgmwy5iL36RycLVaN2UAAAAAQUIPAAAAAABkV/z6ixW03pcxZTQtP8XQ")
                .build();
        Response response = client.newCall(request).execute();

        String string = response.body().string();
        JSONObject jsonObject = JSONObject.parseObject(string);
        JSONArray jsonArray = jsonObject.getJSONArray("data");

        System.out.println("data长度：" + jsonArray.size());
        AtomicReference<Long> a = new AtomicReference<>(0L);
        jsonArray.forEach(data -> {
                    JSONObject json = (JSONObject) data;
                    a.updateAndGet(v -> v + json.getLong("orderCount"));
                }

        );
        System.out.println(a);
    }
}
