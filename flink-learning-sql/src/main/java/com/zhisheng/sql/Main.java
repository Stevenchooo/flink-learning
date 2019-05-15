package com.zhisheng.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * @author Administrator
 */
public class Main {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //基于EventTime进行处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.199.188:9092");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("group.id", "groupIdOne");


        //Stream1，从Kafka中读取数据   Tuple3<String, String, String>
        DataStream<String> kafkaStream = env.addSource(new FlinkKafkaConsumer010<>("world",
                new SimpleStringSchema(), properties));


        DataStream<Tuple3<String, String, String>> tableStream =
                kafkaStream.map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String s) throws Exception {
                        String[] word = s.split(",");

                        return new Tuple3<>(word[0], word[1], word[2]);
                    }
                });

        //将Stream1注册为Table1
        tableEnv.registerDataStream("Table1", tableStream, "name, age, sexy, proctime.proctime");

        //Stream2，从Socket中读取数据
        DataStream<Tuple2<String, String>> socketStream = env.socketTextStream("192.168.199.61", 9001, "\n").
                map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        String[] words = s.split("\\s");
                        if (words.length < 2) {
                            return new Tuple2<>();
                        }

                        return new Tuple2<>(words[0], words[1]);
                    }
                });

        //将Stream2注册为Table2
        tableEnv.registerDataStream("Table2", socketStream, "name, job, proctime.proctime");

        //执行SQL Join进行联合查询
        Table result = tableEnv.sqlQuery("SELECT DISTINCT t1.name, t1.age, t1.sexy, t2.job, t2.proctime as shiptime\n" +
                "FROM Table1 AS t1\n" +
                "JOIN Table2 AS t2\n" +
                "ON t1.name = t2.name\n" +
//                "");
                "AND t1.proctime BETWEEN t2.proctime - INTERVAL '1' SECOND AND t2.proctime + INTERVAL '1' SECOND");

        //将查询结果转换为Stream，并打印输出
        tableEnv.toRetractStream(result, Row.class).print();

        env.execute();
    }
}