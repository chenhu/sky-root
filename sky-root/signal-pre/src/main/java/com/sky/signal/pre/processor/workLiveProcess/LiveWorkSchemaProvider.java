package com.sky.signal.pre.processor.workLiveProcess;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: 职住分析相关的Schema定义
 */
public class LiveWorkSchemaProvider {

    /**
    * description: 职住分析基础SCHEMA,居住地和工作地都要用到
    * param:
    * return:
    **/
    public static final StructType BASE_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
            DataTypes.createStructField("stay_time", DataTypes.DoubleType, false)));

    /**
    * description: 职住分析结果
    * param:
    * return:
    **/
    public static final StructType WORK_LIVE_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("cen_region", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age", DataTypes.ShortType, false),
            DataTypes.createStructField("stay_time", DataTypes.DoubleType, false),
            DataTypes.createStructField("exists_days", DataTypes.LongType, false),
            DataTypes.createStructField("live_base", DataTypes.StringType, true),
            DataTypes.createStructField("live_lng", DataTypes.DoubleType, true),
            DataTypes.createStructField("live_lat", DataTypes.DoubleType, true),
            DataTypes.createStructField("on_lsd", DataTypes.LongType, true),
            DataTypes.createStructField("uld", DataTypes.LongType, true),
            DataTypes.createStructField("work_base", DataTypes.StringType, true),
            DataTypes.createStructField("work_lng", DataTypes.DoubleType, true),
            DataTypes.createStructField("work_lat", DataTypes.DoubleType, true),
            DataTypes.createStructField("on_wsd", DataTypes.LongType, true),
            DataTypes.createStructField("uwd", DataTypes.LongType, true)));

    public static final StructType WORK_LIVE_CELL_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("stay_time", DataTypes.DoubleType, false),
            DataTypes.createStructField("days", DataTypes.LongType, false)));
    public static final StructType EXIST_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("cen_region", DataTypes.IntegerType, false),// 户籍所在地
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age", DataTypes.ShortType, false),
            DataTypes.createStructField("sum_time", DataTypes.LongType, false)));
    public static final StructType ULD_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("uld", DataTypes.LongType, false)));

    public static final StructType UWD_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("uwd", DataTypes.LongType, false)));
}
