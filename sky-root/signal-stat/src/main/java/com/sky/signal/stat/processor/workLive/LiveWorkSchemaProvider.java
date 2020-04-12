package com.sky.signal.stat.processor.workLive;

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
    * description: 职住分析结果，包含一些简单的统计信息
    * param:
    * return:
    **/
    public static final StructType WORK_LIVE_STAT_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("js_region", DataTypes.IntegerType, false),
            DataTypes.createStructField("cen_region", DataTypes.StringType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age", DataTypes.ShortType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("stay_time", DataTypes.DoubleType, false),
            DataTypes.createStructField("stay_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("exists_days", DataTypes.LongType, false),
            DataTypes.createStructField("live_base", DataTypes.StringType, false),
            DataTypes.createStructField("live_geo", DataTypes.StringType, true),
            DataTypes.createStructField("live_lng", DataTypes.DoubleType, true),
            DataTypes.createStructField("live_lat", DataTypes.DoubleType, true),
            DataTypes.createStructField("on_lsd", DataTypes.LongType, false),
            DataTypes.createStructField("uld", DataTypes.LongType, false),
            DataTypes.createStructField("work_base", DataTypes.StringType, false),
            DataTypes.createStructField("work_geo", DataTypes.StringType, true),
            DataTypes.createStructField("work_lng", DataTypes.DoubleType, true),
            DataTypes.createStructField("work_lat", DataTypes.DoubleType, true),
            DataTypes.createStructField("on_wsd", DataTypes.LongType, false),
            DataTypes.createStructField("uwd", DataTypes.LongType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false)));

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

}
