package com.example.demo.dot;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SourceFun implements SourceFunction<Event> {

    public static boolean running = true;
    public static String[] users = {"叶沐晨", "君沫邪", "若如梦"};
    public static String[] urls = {"m.com", "a.com", "c.com"};

    @Override
    public void run(SourceContext sourceContext) {

        //随机生成数据
        Random random = new Random();
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Event event = new Event(user, url, System.currentTimeMillis());
            sourceContext.collect(event);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
