package com.example.demo.cdc;


import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffle;
import org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffleProducer;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2024/2/12 19:17
 */
public class FlinkCdcTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "applet");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/flink/checkpoint");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop7")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("applet_user")
                .tableList("applet_user.ware_sku")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);

        stringDataStreamSource.print();
        stringDataStreamSource.sinkTo(getKafkaSink("applet_flink"));

        env.execute("mysql cdc :");

    }

    public static KafkaSink<String> getKafkaSink(String topicName) {
        return KafkaSink.<String>builder()
                .setBootstrapServers("hadoop1:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("applet-" + topicName + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }


    /**
     *
     *Look, if you had one shot or one opportunity
     *To seize everything you ever wanted in one moment
     *Would you capture it or just let it slip?
     *
     *
     * Yo, his palms are sweaty, knees weak, arms are heavy
     * There is vomit on his sweater already
     * Mom's spaghetti he's nervous
     * But on the surface he looks calm and ready
     * To drops bombs, but he keeps on forgetting
     * What he wrote down, the whole crowd goes so loud
     * He opens his mouth but the words won't come out
     * He's choking, how? Everybody's jokin' now
     * The clock's run out, time's up, over BLOW!
     * Snap back to reality, oh there goes gravity
     * Oh, there goes Rabbit, he choked, he's so mad
     * But he won't give up that easy, no he won't have it
     * He knows his whole back's to these ropes
     * It don't matter, he's dope, he knows that
     * But he's broke, he's so sad that he knows
     * When he goes back to this mobile home
     * That's when it's back to the lab again, yo
     * This whole rhapsody, better go capture this moment
     * And hope it don't collapse on him
     *
     *You better lose yourself in the music
     * The moment you own it you better never let it go, oh
     * You only get one shot, do not miss your chance to blow
     * Cuz opportunity comes once in a lifetime, yo
     * You better lose yourself in the music
     * The moment you own it you better never let it go, oh
     * You only get one shot, do not miss your chance to blow
     * Cuz opportunity comes once in a lifetime, yo
     * You better
     *
     *
     *Soul's escapin' through this hole's that is gaping
     * This world is mines for the taking
     * Make me king as we move toward a new world order
     * A normal life is boring
     * But superstardom's close to post mortem
     * It only grows harder, homie grows hotter
     * He blows us all over, these hoes is all on him
     * Coast to coast shows, he's known as the Globetrotter
     * Lonely roads got him
     * He knows he's grown farther from home, he's no father
     * He goes home and barely knows his own daughter
     * But hold ya nose cuz here goes the cold water
     * These hoes don't want him no mo', he's cold prada
     * They moved on to the next shmo who flows
     * Who nose dove and sold nada
     * And so the so proper
     * His toll, it unfolds and I suppose it's old, partner
     * But the beat goes on
     * Duh duh doe, duh doe, dah dah dah dah
     *
     *
     * No more games, I'ma change for due called rage
     * Tear this muthafuckin' roof off like two dogs caged
     * I was playin' in the beginnin', the mood all changed
     * I've been chewed up and spit out and booed off stage
     * But I kept rhymin' and stepped writin' the next cipher
     * Best believe somebody's payin' the pied piper
     * All the pain inside amplified by the
     * Fact that I can't get by with my nine to five
     * And I can't provide the right type of life for my family
     * Cuz, man, these goddamn food stamps don't buy diapers
     * And there's no movie, there's no Mekhi Pfifer
     * This is my life and these times are so hard
     * And it's gettin' even harder tryin' to feed and water
     * My seed plus teeter-totter
     * Caught up between bein' a father and a pre-madonna
     * Baby momma drama, screamin' on her
     * Too much for me to wanna stay in one spot
     * Another damn or not has gotten me to the point
     * I'm like a snail, I've got to formulate a plot
     * Or end up in jail or shot
     * Success is my only muthafuckin' option, failure's not
     * Momma love you but this trailer's got to go
     * I cannot grow old in Salem's Lot
     * So here I go, it's my shot
     * Feet fail me not
     * Cuz maybe the only opportunity that I got
     *
     * Duh doo
     * You can do anything you set your mind to, man
     */
}
