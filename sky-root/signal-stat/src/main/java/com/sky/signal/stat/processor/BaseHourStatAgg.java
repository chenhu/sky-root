package com.sky.signal.stat.processor;

import com.google.common.collect.Lists;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
* description: 基站每小时人口特征统计
* param:
* return:
**/
@Service("baseHourStatAgg")
public class BaseHourStatAgg implements Serializable {
    private static final StructType SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("time_inter", DataTypes.IntegerType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("js_region", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("peo_num", DataTypes.LongType, false)
    ));
    @Autowired
    private transient ParamProperties params;
    public DataFrame process() {
        DataFrame aggDf = FileUtil.readFile(FileUtil.FileType.CSV, SCHEMA, params.getSavePath() + "stat/*/base-hour");
        aggDf = aggDf.groupBy("date", "base", "time_inter", "person_class", "js_region", "sex", "age_class").agg(sum("peo_num")
                .as("peo_num")).orderBy(col("date"),col("base"),col("time_inter"),col("person_class"),col("js_region"), col("sex"), col
                ("age_class"));
        FileUtil.saveFile(aggDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/base-hour");
        return aggDf;
    }
}
