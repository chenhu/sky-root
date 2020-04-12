package com.sky.signal.stat.processor.od;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: OD分析相关的Schema定义
 */
public class ODSchemaProvider {

    public static final StructType OD_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
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

    public static final StructType OD_STAT_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("leave_base", DataTypes.StringType, true),
            DataTypes.createStructField("leave_geo", DataTypes.StringType, true),
            DataTypes.createStructField("arrive_base", DataTypes.StringType, true),
            DataTypes.createStructField("arrive_geo", DataTypes.StringType, true),
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
            DataTypes.createStructField("live_base", DataTypes.StringType, true),
            DataTypes.createStructField("live_geo", DataTypes.StringType, true),
            DataTypes.createStructField("work_base", DataTypes.StringType, true),
            DataTypes.createStructField("work_geo", DataTypes.StringType, true),
            DataTypes.createStructField("trip_purpose", DataTypes.ShortType, false)
    ));
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
    public static final StructType OD_TRACE_STAT_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("leave_base", DataTypes.StringType, false),
            DataTypes.createStructField("arrive_base", DataTypes.StringType, false),
            DataTypes.createStructField("from_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("arrive_time_class", DataTypes.IntegerType, false)));
    public static final StructType OD_TRACE_STAT_SCHEMA1 = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("leave_base", DataTypes.StringType, false),
            DataTypes.createStructField("arrive_base", DataTypes.StringType, false),
            DataTypes.createStructField("from_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("arrive_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("trip_num", DataTypes.LongType, false),
            DataTypes.createStructField("num_inter", DataTypes.LongType, false)
    ));

    public static final StructType OD_STAT_TEMP_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("linked_distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("distance_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("max_speed_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("cov_speed_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("trip_purpose", DataTypes.ShortType, false)
    ));


    public static final StructType OD_TEMP_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("leave_geo", DataTypes.StringType, true),
            DataTypes.createStructField("arrive_geo", DataTypes.StringType, true),
            DataTypes.createStructField("leaveTime_inter", DataTypes.TimestampType, false),
            DataTypes.createStructField("arriveTime_inter", DataTypes.TimestampType, false),
            DataTypes.createStructField("distance", DataTypes.IntegerType, false),
            DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("trip_purpose", DataTypes.ShortType, false)
    ));

    public static final StructType OD_TRIP_STAT_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("has_trip", DataTypes.ByteType, false),
            DataTypes.createStructField("staypoint_count", DataTypes.IntegerType, false)));
    public static final StructType OD_TRIP_STAT_SCHEMA1 = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("has_trip", DataTypes.ByteType, false),
            DataTypes.createStructField("staypoint_count", DataTypes.IntegerType, false)));
    public static final StructType OD_TRIP_STAT_SCHEMA2 = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("trip_class", DataTypes.IntegerType, false)));

}
