package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author applet
 * @date 2023/5/14
 */
public class test {

    public static void main(String[] args) throws IOException, InterruptedException {
        String ME1 = "376523290039260";
        String ME2 = "380189367844822";
        for (int i = 0; i < 10000; i++) {

            String username0 = "test010cny";
            String username1 = "test006inr";
            String username2 = "test007thb";
            String username3 = "test008gbp";
            String username4 = "test009gyp";
            String username5 = "test001usd";
            String username6 = "test002hkd";
            String username7 = "test003vnd";
            String username8 = "test004twd";
            String username9 = "test005myr";

            String username10 = "test011sgd";
            String username11 = "test012eur";
            String username12 = "test013php";
            String username13 = "test014krw";
            String username14 = "test015aud";
            String username15 = "test016cad";

            final String[] token = {
                    getToken(username0, ME1),
                    getToken(username1, ME1),
                    getToken(username2, ME1),
                    getToken(username3, ME1),
                    getToken(username4, ME1),
                    getToken(username5, ME1),
                    getToken(username6, ME1),
                    getToken(username7, ME1),
                    getToken(username8, ME1),
                    getToken(username9, ME1),
                    getToken(username10, ME2),
                    getToken(username11, ME2),
                    getToken(username12, ME2),
                    getToken(username13, ME2),
                    getToken(username14, ME2),
                    getToken(username15, ME2)
            };
            int finalI = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[0], finalI, username0);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[0] = getToken(username0, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[1], finalI, username1);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[1] = getToken(username1, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[2], finalI, username2);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[2] = getToken(username2, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[3], finalI, username3);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[3] = getToken(username3, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[4], finalI, username4);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[4] = getToken(username4, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[5], finalI, username5);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[5] = getToken(username5, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[6], finalI, username6);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[6] = getToken(username6, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[7], finalI, username7);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[7] = getToken(username7, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[8], finalI, username8);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[8] = getToken(username8, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[9], finalI, username9);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[9] = getToken(username9, ME1);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();


            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[10], finalI, username10);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[10] = getToken(username10, ME2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[11], finalI, username11);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[11] = getToken(username11, ME2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[12], finalI, username12);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[12] = getToken(username12, ME2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[13], finalI, username13);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[13] = getToken(username13, ME2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[14], finalI, username14);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[14] = getToken(username14, ME2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        JSONObject bet = bet(token[15], finalI, username15);
                        String status = bet.getString("status");
                        String data = bet.getString("data");
                        if (status.equals("false") && data.equals("token")) {
                            token[15] = getToken(username15, ME2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            Thread.sleep(2020);
        }


    }


    public static String getToken(String username, String metchat) throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "");
        Request request = new Request.Builder()
                .url("http://bobweb.oqwelo123.com/cApi/v2/member/login?username=" + username + "&password=test311&merchant=" + metchat + "&demo=0")
                .addHeader("Accept", "application/json, text/plain, */*")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.9")
                .addHeader("Connection", "keep-alive")
                .addHeader("Content-Type", "application/json")
                .addHeader("Cookie", "SESSION=M2E2NjIzNWEtMzAzZS00YjkxLWEyZjQtMzdiZjQ2MjYzN2Zj")
                .addHeader("Origin", "http://test-external-admin-web.bdgatewaynet.com")
                .addHeader("Referer", "http://test-external-admin-web.bdgatewaynet.com/login.html?redirect=/manager/user")
                .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36")
                .build();
        Response response = client.newCall(request).execute();
        ResponseBody body1 = response.body();
        String string = body1.string();
        JSONObject jsonObject = JSON.parseObject(string);
        JSONObject data = jsonObject.getJSONObject("data");
        String token = data.getString("token");
        return token;
    }

    public static JSONObject bet(String token, int finalI, String username) throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        int num = getNum();
        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
        RequestBody body = RequestBody.create(mediaType, "c=1&b[0]=mch=70101089974415515%26mkt=70101243066693215%26oid=70101243068623447%26odd=1.010%26a=" + num + "%26bt=1&types=1");
        Request request = new Request.Builder()
                .url("https://duatbob-txapi.kkgnru.com/game/bet")
                .method("POST", body)
                .addHeader("Accept", "application/json, text/plain, */*")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.9")
                .addHeader("Connection", "keep-alive")
                .addHeader("Content-Type", "application/x-www-form-urlencoded")
                .addHeader("Origin", "http://www.zxjlbvip.org")
                .addHeader("Referer", "http://www.zxjlbvip.org/")
                .addHeader("User-Agent", "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Mobile Safari/537.36")
                .addHeader("device", "1")
                .addHeader("lang", "cn")
                .addHeader("token", token)
                .build();
        Response response = client.newCall(request).execute();
        String string = response.body().string();
        JSONObject jsonObject = JSON.parseObject(string);
        String status = jsonObject.getString("status");

        System.out.println(finalI + "投注用户：" + username + " 投注状态：" + status + " 投注结果：" + jsonObject.getString("data") + " 投注金额：" + num + "     整体结果：" + jsonObject);
        return jsonObject;
    }

    public static int getNum() {
        int v = (int) (190 + Math.random() * (20 - 1 + 1));
        return v;
    }

}
