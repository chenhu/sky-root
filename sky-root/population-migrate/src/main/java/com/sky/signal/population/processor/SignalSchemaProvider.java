package com.sky.signal.population.processor;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: 信令相关的Schema定义
 */
public class SignalSchemaProvider {
    //区县OD分析数据源格式
    public static final StructType OD_SCHEMA = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_base", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_lng", DataTypes.DoubleType, false),
                    DataTypes.createStructField("leave_lat", DataTypes.DoubleType, false),
                    DataTypes.createStructField("arrive_base", DataTypes.StringType, false),
                    DataTypes.createStructField("arrive_lng", DataTypes.DoubleType, false),
                    DataTypes.createStructField("arrive_lat", DataTypes.DoubleType, false),
                    DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("linked_distance", DataTypes.IntegerType, false),
                    DataTypes.createStructField("max_speed", DataTypes.DoubleType, false),
                    DataTypes.createStructField("cov_speed", DataTypes.DoubleType, false),
                    DataTypes.createStructField("distance", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));
    //原始数据合并了基站后的数据
    public static final StructType SIGNAL_SCHEMA_BASE = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),// 号码归属地
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),// 上报地市
            DataTypes.createStructField("district", DataTypes.StringType, false), // 区县编码
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false)));
    //区县用户活动记录
    public static final StructType DISTRICT_SIGNAL = DataTypes.createStructType
            (Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),// 号码归属地
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),// 上报地市
            DataTypes.createStructField("district", DataTypes.StringType, false), // 区县编码
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));
    //区县OD
    public static final StructType DISTRICT_OD = DataTypes.createStructType
            (Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),// 号码归属地
            DataTypes.createStructField("city_code_o", DataTypes.IntegerType, false),
            DataTypes.createStructField("district_o", DataTypes.StringType, false),
            DataTypes.createStructField("city_code_d", DataTypes.IntegerType, false),
            DataTypes.createStructField("district_d", DataTypes.StringType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));


}
