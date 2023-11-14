package com.example.demo;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;

public class DemoApplication {

    public static void main(String[] args) throws Exception {


        JSONObject aJson = JSONObject.parseObject(a());
        JSONObject bJson = JSONObject.parseObject(b());
        JSONObject cJson = JSONObject.parseObject(c());

        ArrayList<String> strings = new ArrayList<>();


        JSONArray data = aJson.getJSONArray("data");
        data.forEach(jsonObject -> {
            JSONObject json = (JSONObject) jsonObject;
            String merchantName = json.getString("merchantName");
            strings.add(merchantName);
        });

        JSONArray jsonArray = bJson.getJSONObject("data").getJSONArray("list");

        jsonArray.forEach(dataJ -> {

            JSONObject json = (JSONObject) dataJ;
            String merchantName = json.getString("merchantName");
            if (!strings.contains(merchantName)) {
                System.out.println("merchantName = " + merchantName);
            }
        });


        JSONArray cjsonArray = cJson.getJSONObject("data").getJSONArray("list");

        cjsonArray.forEach(dataJ -> {

            JSONObject json = (JSONObject) dataJ;
            String merchantName = json.getString("merchantName");
            if (!strings.contains(merchantName)) {
                System.out.println("merchantName = " + merchantName);
            }
        });
        System.out.println("============");
    }


    public static String a() throws IOException {
        String text = "Java是最好的语言！";
        File file = new File("F:\\a\\a.sql");
        //如果文件不存在则创建文件
        if (!file.exists()) {
            file.createNewFile();
        }

        //获取要被写入内容的文件（将内容写入文件）
        //如果构造方法的第二个参数为true，则新内容会添加到旧内容之后
        //						为false，或者不写第二个参数，则原文件内容会被替换
        FileWriter writer = new FileWriter(file.getName()/*,true*/);
        //将文本写入文件
        writer.write(text);
        //关闭文件写入对象
        writer.close();

        //创建文件输入字节流对象（获取文件内容）
        InputStream is = new FileInputStream(file);
        //获取当前可读的字节数
        int al = is.available();
        //创建字节数组，用于存放读取的文件内容
        byte[] bytes = new byte[al];
        //读取内容存到byte数组中，并返回实际读取成功的字节数
        int read = is.read(bytes);
        //将字节数组转换为字符串
        String str = new String(bytes);
        //关闭输入流对象
        is.close();
        return str;
    }

    public static String b() throws IOException {
        String text = "Java是最好的语言！";
        File file = new File("F:\\a\\b.sql");
        //如果文件不存在则创建文件
        if (!file.exists()) {
            file.createNewFile();
        }

        //获取要被写入内容的文件（将内容写入文件）
        //如果构造方法的第二个参数为true，则新内容会添加到旧内容之后
        //						为false，或者不写第二个参数，则原文件内容会被替换
        FileWriter writer = new FileWriter(file.getName()/*,true*/);
        //将文本写入文件
        writer.write(text);
        //关闭文件写入对象
        writer.close();

        //创建文件输入字节流对象（获取文件内容）
        InputStream is = new FileInputStream(file);
        //获取当前可读的字节数
        int al = is.available();
        //创建字节数组，用于存放读取的文件内容
        byte[] bytes = new byte[al];
        //读取内容存到byte数组中，并返回实际读取成功的字节数
        int read = is.read(bytes);
        //将字节数组转换为字符串
        String str = new String(bytes);
        //关闭输入流对象
        is.close();
        return str;
    }

    public static String c() throws IOException {
        String text = "Java是最好的语言！";
        File file = new File("F:\\a\\c.sql");
        //如果文件不存在则创建文件
        if (!file.exists()) {
            file.createNewFile();
        }

        //获取要被写入内容的文件（将内容写入文件）
        //如果构造方法的第二个参数为true，则新内容会添加到旧内容之后
        //						为false，或者不写第二个参数，则原文件内容会被替换
        FileWriter writer = new FileWriter(file.getName()/*,true*/);
        //将文本写入文件
        writer.write(text);
        //关闭文件写入对象
        writer.close();

        //创建文件输入字节流对象（获取文件内容）
        InputStream is = Files.newInputStream(file.toPath());
        //获取当前可读的字节数
        int al = is.available();
        //创建字节数组，用于存放读取的文件内容
        byte[] bytes = new byte[al];
        //读取内容存到byte数组中，并返回实际读取成功的字节数
        int read = is.read(bytes);
        //将字节数组转换为字符串
        String str = new String(bytes);
        //关闭输入流对象
        is.close();
        return str;
    }
}


