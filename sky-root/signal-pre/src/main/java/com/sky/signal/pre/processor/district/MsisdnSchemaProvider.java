package com.sky.signal.pre.processor.district;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:58
 * description: 信令相关的Schema定义
 */
public class MsisdnSchemaProvider {
    // 原始数据分日期
    public static final StructType MSISDN = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false)));

}
