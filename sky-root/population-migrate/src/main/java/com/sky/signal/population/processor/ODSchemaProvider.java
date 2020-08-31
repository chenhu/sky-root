package com.sky.signal.population.processor;

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
    public static final StructType POINT_SCHEMA = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("base", DataTypes.StringType, false),
                    DataTypes.createStructField("lng", DataTypes.DoubleType, false),
                    DataTypes.createStructField("lat", DataTypes.DoubleType, false),
                    DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
                    DataTypes.createStructField("district_code", DataTypes.IntegerType, false)));
    public static final StructType POINT_SCHEMA1 = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("begin_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("last_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
                    DataTypes.createStructField("district_code", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration_o", DataTypes.IntegerType, false)
                    ));
    public static final StructType DISTRICT_OD_SCHEMA = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("duration_o", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration_d", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));



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
                    DataTypes.createStructField("duration_o", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration_d", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));
    public static final StructType OD_DISTRICT_SCHEMA = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("duration_o", DataTypes.IntegerType, false)));
    public static final StructType OD_DISTRICT_SCHEMA_O = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("duration_o", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));
    public static final StructType OD_DISTRICT_SCHEMA_DET = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("arrive_time", DataTypes.TimestampType, false),
                    DataTypes.createStructField("duration_o", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration_d", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false)));
    public static final StructType OD_DISTRICT_SCHEMA_CLASSIC = DataTypes.createStructType
            (Lists.newArrayList(DataTypes.createStructField("date", DataTypes.IntegerType, false),
                    DataTypes.createStructField("msisdn", DataTypes.StringType, false),
                    DataTypes.createStructField("leave_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("leave_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_city", DataTypes.IntegerType, false),
                    DataTypes.createStructField("arrive_district", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration_o_classic", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration_d_classic", DataTypes.IntegerType, false),
                    DataTypes.createStructField("move_time_classic", DataTypes.IntegerType, false)));
}
