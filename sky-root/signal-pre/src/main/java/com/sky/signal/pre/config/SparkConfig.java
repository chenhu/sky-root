package com.sky.signal.pre.config;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * 注入spark上下文, 通过上下文访问spark集群
 */
@Data
@Configuration
public class SparkConfig {
    @Autowired
    private ParamProperties params;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf();

        //spark应用名称
        sparkConf.setAppName(params.getAppName());

        //spark主机url
        if (StringUtils.hasLength(params.getMasterUrl())) {
            sparkConf.setMaster(params.getMasterUrl());
        }

        //分区数
//        sparkConf.set("spark.default.parallelism", params.getPartitions().toString());
//        sparkConf.set("spark.sql.shuffle.partitions", params.getPartitions().toString());
//        sparkConf.set("spark.network.timeout", "60000");
        sparkConf.set("spark.locality.wait", "10");


        //使用kryo序列化
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
       // sparkConf.set("spark.kryo.registrationRequired", "true");
        sparkConf.set("spark.kryoserializer.buffer", "64m");
        sparkConf.set("spark.kryoserializer.buffer.max", "256m");

        //kryo序列化需注册类型, 普通类型使用字符串方式注册
        StringBuilder sb = new StringBuilder();
        sb.append("org.apache.spark.sql.catalyst.InternalRow");
        sb.append(",org.apache.spark.sql.catalyst.expressions.UnsafeRow");
        sparkConf.set("spark.kryo.classesToRegister", sb.toString());

        //kryo序列化需注册类型, 数组类型使用类型信息注册
        sparkConf.registerKryoClasses(new Class[]{
               InternalRow[].class, StructField[].class, Object[].class, byte[][].class,String[].class
        });

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SQLContext sqlContext() {
        return new SQLContext(javaSparkContext().sc());
    }
}