package com.sky.signal.population.processor;

import com.google.common.collect.Lists;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:25
 * description: 统一定义基站信息部分的RDD Schema 信息
 */
public class CellSchemaProvider {
    public static final StructType CELL_SCHEMA_OLD = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
            DataTypes.createStructField("district", DataTypes.StringType,
                    false),
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("direction", DataTypes.IntegerType,
                    false),
            DataTypes.createStructField("name", DataTypes.StringType,
                    false),
            DataTypes.createStructField("geohash", DataTypes.StringType, false)
            ));

    public static final StructType CELL_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
            DataTypes.createStructField("district", DataTypes.StringType,
                    false),
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("direction", DataTypes.IntegerType,
                    false),
            DataTypes.createStructField("name", DataTypes.StringType,
                    false),
            DataTypes.createStructField("geohash", DataTypes.StringType, false)));
}
