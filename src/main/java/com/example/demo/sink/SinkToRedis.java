package com.example.demo.sink;


/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/7/17 22:47
 */
public class SinkToRedis {
   /* public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建一个数据源
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "", 1000L),
                new Event("Bob", "", 2000L),
                new Event("ALice", "", 3000L),
                new Event("Bob", "", 3300L),
                new Event("ALice", "", 3200L),
                new Event("Bob", "", 3400L),
                new Event("Bob", "", 3800L),
                new Event("Bob", "", 4200L)
        );

        // 创建一个redis链接
        FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder();
        builder.setHost("127.0.0.1:6379");
        FlinkJedisPoolConfig build = builder.build();

        stream.addSink(new RedisSink<>(build, new RedisMapper<Event>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"event");
            }

            @Override
            public String getKeyFromData(Event event) {
                return event.username;
            }

            @Override
            public String getValueFromData(Event event) {
                return event.url;
            }
        }));
        env.execute();
    }*/

}
