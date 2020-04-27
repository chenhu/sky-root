package com.sky.signal.stat.processor;

import com.google.common.collect.Lists;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

/**
 * 对外人口流动数据
 */
@Service("migrateStat")
public class MigrateStat implements Serializable {
    private static final StructType SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("peo_num", DataTypes.LongType, false)
    ));
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame validDF, DataFrame workLiveDF, int batchId) {
        DataFrame joinedDf = validDF.join(workLiveDF, validDF.col("msisdn").equalTo(workLiveDF.col("msisdn")), "left_outer")
                .select(
                        validDF.col("date"),
                        validDF.col("msisdn"),
                        workLiveDF.col("region")
                );
        joinedDf = joinedDf.groupBy("date", "region").agg(countDistinct("msisdn")
                .as("peo_num")).orderBy(col("date"),col("region"));
        FileUtil.saveFile(joinedDf, FileUtil.FileType.CSV, params.getSavePath() + "stat/" + batchId + "/migrate-stat");
        return joinedDf;
    }
    public DataFrame agg() {
        DataFrame aggDf = FileUtil.readFile(FileUtil.FileType.CSV, SCHEMA, params.getSavePath() + "stat/*/migrate-stat");
        aggDf = aggDf.groupBy("date", "region").agg(sum("peo_num")
                .as("peo_num")).orderBy(col("date"),col("region"));
        FileUtil.saveFile(aggDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/migrate-stat");
        return aggDf;
    }
}
