package com.zhisheng.data.sinks;


import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.data.sinks.model.Student;
import com.zhisheng.data.sinks.sinks.SinkToMySQL;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.WORLD_TOPIC;

/**
 * @author Administrator
 */
public class Main {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(WORLD_TOPIC),
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, Student.class));
        //博客里面用的是 fastjson，这里用的是gson解析，解析字符串成 student 对象

        // 数据Sink到Mysql
        student.addSink(new SinkToMySQL());

        env.execute("Flink data sink");
    }
}