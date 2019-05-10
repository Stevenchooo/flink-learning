package com.zhisheng.data.sources.utils;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Desc: 往kafka中写数据,可以使用这个main函数进行测试
 * @author Administrator
 */
public class KafkaUtil {
    private static final String BROKER_LIST = "192.168.199.188:9092";
    private static final String TOPIC = "world";

    private static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        MetricEvent metric = new MetricEvent();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<>(3);
        Map<String, Object> fields = new HashMap<>(5);

        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, null, null, GsonUtil.toJson(metric));
        producer.send(record);
        System.out.println("发送数据: " + GsonUtil.toJson(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
