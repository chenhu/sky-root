package com.sky.signal.pre.processor.dataquality;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: 数据质量相关的Schema定义
 */
public class DataQualitySchemaProvider {
    //原始数据合并了基站后的数据
    public static final StructType SIGNAL_SCHEMA_BASE = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("orginal_count", DataTypes.LongType, false),
            DataTypes.createStructField("valid_row_count", DataTypes.LongType, false),
            DataTypes.createStructField("base_count", DataTypes.LongType, false),
            DataTypes.createStructField("no_crm_count", DataTypes.LongType, false),
            DataTypes.createStructField("dt_error_count1", DataTypes.LongType, false),
            DataTypes.createStructField("dt_error_count2", DataTypes.LongType, false),
            DataTypes.createStructField("no_base_count", DataTypes.LongType, false),
            DataTypes.createStructField("msisdn_count", DataTypes.LongType, false)
    ));
}
