package com.example.demo.controller;

import okhttp3.*;

import java.io.IOException;

/**
 * @author applet
 * @date 2023/5/14
 */
public class Re {
    public static void main(String[] args) throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\n  \"uuid\": \"asdjkslajd1\",\n  \"player\": {\n    \"id\": \"asdjkslajd1\",\n    \"update\": false,\n    \"language\": \"zh\",\n    \"country\": \"CN\",\n    \"currency\": \"CNY\",\n    \"session\": {\n      \"id\": \"asdjkslajd1\",\n      \"ip\": \"127.0.0.1\"\n    }\n  },\n  \"config\": {\n    \"game\": {\n      \"table\": {\n        \"id\": \"scudamore0000000\"\n      }\n    },\n    \"channel\": {\n      \"wrapped\": false,\n      \"mobile\": false\n    },\n    \"freeGames\": false\n  }\n}");
        Request request = new Request.Builder()
                .url("https://tmobgm.uat1.evo-test.com/ua/v1/tmobgm0000000001/test123")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();
        System.out.println("response = " + response.body().bytes());

    }
}
