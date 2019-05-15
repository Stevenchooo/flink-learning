package com.zhisheng.producer;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;
import java.util.Random;

/**
 * @program: flink-learning
 * @description:
 * @author: Steven
 * @create: 2019-05-15 14:17
 **/

public class WriteIntoKafka {

    public static void main(String[] args) throws Exception {

        // 构造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发度
        env.setParallelism(1);

        // 构造流图，将自定义Source生成的数据写入Kafka
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.199.188:9092");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("group.id", "groupIdOne");

        FlinkKafkaProducer010<String> producer = new FlinkKafkaProducer010<String>(
                "world", new SimpleStringSchema(), properties);


        FlinkKafkaProducer010.writeToKafkaWithTimestamps(
                messageStream, "world",
                new SimpleStringSchema(), properties);

        messageStream.addSink(producer);


        // 调用execute触发执行
        env.execute();
    }

    // 自定义Source，每隔1s持续产生消息
    public static class SimpleStringGenerator implements SourceFunction<String> {
        static final String[] NAME = {"Carry", "Alen", "Mike", "Ian", "John", "Kobe", "James"};

        static final String[] SEX = {"MALE", "FEMALE"};

        static final int COUNT = NAME.length;

        boolean running = true;

        Random rand = new Random(47);

        @Override
        //rand随机产生名字，性别，年龄的组合信息
        public void run(SourceContext<String> ctx) throws Exception {

            while (running) {

                int i = rand.nextInt(COUNT);

                int age = rand.nextInt(70);

                String sexy = SEX[rand.nextInt(2)];

                ctx.collect(NAME[i] + "," + age + "," + sexy);

                new Thread().sleep(1000);

            }

        }

        @Override

        public void cancel() {

            running = false;

        }

    }

}
