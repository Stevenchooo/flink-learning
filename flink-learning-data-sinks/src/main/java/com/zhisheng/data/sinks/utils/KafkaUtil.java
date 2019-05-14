package com.zhisheng.data.sinks.utils;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.data.sinks.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.WORLD_TOPIC;

/**
 * Desc: 往kafka中写数据,可以使用这个main函数进行测试
 * @author Administrator
 */
public class KafkaUtil {
    private static final String BROKER_LIST = "192.168.199.188:9092";

    //kafka topic 需要和 flink 程序用同一个 topic

    private static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "zhisheng" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(WORLD_TOPIC, null, null, GsonUtil.toJson(student));
            producer.send(record);
            System.out.println("发送数据: " + GsonUtil.toJson(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
