package com.sky.signal.pre.processor.odAnalyze;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: OD分析相关的Schema定义
 */
public class ODSchemaProvider {
    //原始数据合并了基站后的数据
    public static final StructType TRACE_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("speed", DataTypes.DoubleType, false),
            DataTypes.createStructField("point_type", DataTypes.ByteType, false)));
    public static final StructType OD_TRACE_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("leave_base", DataTypes.StringType, false),
            DataTypes.createStructField("leave_lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("leave_lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("arrive_base", DataTypes.StringType, false),
            DataTypes.createStructField("arrive_lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("arrive_lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false)));
    public static final StructType OD_LINK_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
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

    public static final StructType OD_TEMP_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("leave_base", DataTypes.StringType, true),
            DataTypes.createStructField("arrive_base", DataTypes.StringType, true),
            DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("linked_distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("max_speed", DataTypes.DoubleType, false),
            DataTypes.createStructField("cov_speed", DataTypes.DoubleType, false),
            DataTypes.createStructField("distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("live_base", DataTypes.StringType, false),
            DataTypes.createStructField("work_base", DataTypes.StringType, false)
    ));
    public static final StructType OD_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("leave_base", DataTypes.StringType, true),
            DataTypes.createStructField("arrive_base", DataTypes.StringType, true),
            DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("linked_distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("max_speed", DataTypes.DoubleType, false),
            DataTypes.createStructField("cov_speed", DataTypes.DoubleType, false),
            DataTypes.createStructField("distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("trip_purpose", DataTypes.ShortType, false)
    ));
}
