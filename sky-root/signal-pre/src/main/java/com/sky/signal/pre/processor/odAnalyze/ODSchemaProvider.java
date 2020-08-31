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
    //带停留点类型的轨迹
    public static final StructType TRACE_SCHEMA = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("base", DataTypes.StringType, false),
                    DataTypes.createStructField("lng", DataTypes.DoubleType, false),
                    DataTypes.createStructField("lat", DataTypes.DoubleType, false),
                    DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
                    DataTypes.createStructField("district_code", DataTypes.IntegerType, false),
                    DataTypes.createStructField("distance", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
                    DataTypes.createStructField("speed", DataTypes.DoubleType, false),
                    DataTypes.createStructField("point_type", DataTypes.ByteType, false)));

}
