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

import static org.apache.spark.sql.functions.*;

/**
* description: 按天统计人口分类、有效信令条数为1条、手机号码数据
* param:
* return:
**/
@Service("validSignalStat")
public class ValidSignalStat implements Serializable {
    private static final StructType SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("peo_num", DataTypes.IntegerType, false)));
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame validDF, DataFrame workLiveDF, Integer batchId) {
        validDF = validDF.groupBy("date","msisdn").agg(count("msisdn").as("signal_count")).filter(col("signal_count").equalTo(1));
        DataFrame joinedDf = validDF.join(workLiveDF,validDF.col("msisdn").equalTo(workLiveDF.col("msisdn")),"left_outer");
        joinedDf = joinedDf.select(validDF.col("date"),validDF.col("msisdn"),workLiveDF.col("person_class"))
                .groupBy("date","person_class")
                .agg(countDistinct("msisdn").as("peo_num"));
        FileUtil.saveFile(joinedDf.repartition(params.getStatpatitions()), FileUtil.FileType.PARQUET, params.getValidSignalStatSavePath(batchId.toString()));
        return joinedDf;
    }
    public DataFrame agg() {
        DataFrame aggDf = FileUtil.readFile(FileUtil.FileType.PARQUET, SCHEMA, params.getValidSignalStatSavePath("*"));
        aggDf = aggDf.groupBy("date", "person_class").agg(sum("peo_num")
                .as("peo_num")).orderBy(col("date"),col("person_class"));
        FileUtil.saveFile(aggDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getValidSignalStatSavePath());
        return aggDf;
    }
}
