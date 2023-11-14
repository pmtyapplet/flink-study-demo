package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author applet
 * @date 2023/5/14
 */
public class uattest {

    public static void main(String[] args) throws IOException, InterruptedException {
        task();
    }

    public static void task() throws IOException, InterruptedException {
        String merchatCode1 = "10987792537743756";
        String merchatCode2 = "5484968865459134";
        String merchatCode3 = "5485309512657280";
        String merchatCode4 = "5485342000617088";
        String username1 = "cny";
        String username2 = "cny1";
        String username3 = "cny2";
        String username5 = "cny3";
        String username6 = "cny4";
        String username7 = "cny5";
        String username8 = "cny6";
        String username9 = "cny7";
        String username10 = "cny8";
        String username11 = "cny9";
        String username12 = "cny1";
        String username13 = "027cny";
        String username14 = "028cny";
        String username15 = "029cny";
        String username4 = "030cny";


        String username17 = "006gbp";
        String username18 = "007eur";
        String username19 = "008twd";
        String username20 = "009jyp";
        String username16 = "010php";


        String username22 = "011krw";
        String username23 = "012aud";
        String username24 = "013cad";
        String username25 = "014rub";
        String username21 = "015idr";


        String username27 = "016myr";
        String username28 = "017inr";
        String username29 = "018thb";
        String username30 = "019mxn";
        String username26 = "020egp";


        List<String> strings = Arrays.asList(


                "c=1&b[0]=mch=194271996779680078%26mkt=140133813772083491%26oid=140702510427500483%26odd=5.400%26a=13%26bt=1&types=1"
        );


        int numFail1 = 0;

        for (int i = 0; i < 500; i++) {


            int finalI1 = i;
            new Thread(new Runnable() {
                @Override
                public void run() {

                    try {
                        String token = getToken(username1, merchatCode1);

                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username1, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username2, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username2, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username3, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username3, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username5, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username5, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username6, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username6, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username7, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username7, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username8, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username8, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username9, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username9, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username10, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username10, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
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
                        String token = getToken(username11, merchatCode1);
                        for (int j = 0; j < strings.size(); j++) {
                            String bet = bet(token, finalI1, username11, numFail1, strings.get(j));
                            System.out.println("当前盘口值下标：" + j + " 当前盘口是否成功：" + bet);
                        }


                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).start();

            Thread.sleep(8000);
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
                .addHeader("Cache-Control", "no-cache")
                .addHeader("Connection", "keep-alive")
                .addHeader("Content-Type", "application/json;charset=utf-8")
                .addHeader("Origin", "http://loginbob.oqwelo123.com")
                .addHeader("Pragma", "no-cache")
                .addHeader("Referer", "http://loginbob.oqwelo123.com/")
                .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
                .build();
        Response response = client.newCall(request).execute();
        ResponseBody body1 = response.body();
        String string = body1.string();
        JSONObject jsonObject = JSON.parseObject(string);
        String status = jsonObject.getString("status");
        if (status.equals("true")) {
            JSONObject data = jsonObject.getJSONObject("data");
            String token = data.getString("token");
            return token;
        } else {
            System.out.println("登录错误：" + jsonObject);
            return "";
        }

    }

    public static String bet(String token, int finalI, String username, Integer numFail, String odds) throws IOException {

        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        int num = getNum();
        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");

        RequestBody body = RequestBody.create(mediaType, odds);
        Request request = new Request.Builder()
                .url("https://duatbob-txapi.kkgnru.com/game/bet")
                .method("POST", body)
                .addHeader("Accept", "application/json, text/plain, */*")
                .addHeader("Accept-Language", "zh-CN,zh;q=0.9")
                .addHeader("Cache-Control", "no-cache")
                .addHeader("Connection", "keep-alive")
                .addHeader("Content-Type", "application/x-www-form-urlencoded")
                .addHeader("Origin", "http://bobweb.oqwelo123.com")
                .addHeader("Pragma", "no-cache")
                .addHeader("Referer", "http://bobweb.oqwelo123.com/")
                .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
                .addHeader("device", "1")
                .addHeader("lang", "cn")
                .addHeader("token", token)
                .build();
        Response response = client.newCall(request).execute();
        String string = response.body().string();
        JSONObject jsonObject = JSON.parseObject(string);
        String status = jsonObject.getString("status");

        String data = jsonObject.getString("data");
        if (status.equals("false")) {
            //失败了
            if (data.contains("赛事未开赛")) {
                numFail++;
                BigDecimal divide = new BigDecimal(numFail).divide(new BigDecimal((finalI + 1) * 3), 4, RoundingMode.UP).multiply(new BigDecimal(100));
                System.out.println("当前用户" + "投注失败！当前失败次数：" + numFail + " 当前总下注次数：" + ((finalI + 1) * 3) + " 当前失败率：" + divide + "%");
            }
        }

        System.out.println(finalI + "投注用户：" + username + " 投注状态：" + status + " 投注结果：" + jsonObject.getString("data") + "     整体结果：" + jsonObject);
        return status;
    }

    public static int getNum() {
        int v = (int) (10 + Math.random() * (100 - 10 + 1));
        return v;
    }


}
