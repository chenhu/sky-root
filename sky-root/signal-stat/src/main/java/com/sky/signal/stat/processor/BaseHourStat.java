package com.sky.signal.stat.processor;

import com.google.common.collect.Lists;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.ProfileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

/**
* description: 基站每小时人口特征统计
* param:
* return:
**/
@Service("baseHourStat")
public class BaseHourStat implements Serializable {
    private static final StructType SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("time_inter", DataTypes.IntegerType, false)));
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame validDF, DataFrame workLiveDF) {

        JavaRDD<Row> rdd = validDF.javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                List<Row> rows = new ArrayList<>();
                DateTime begin_time = new DateTime(row.getAs("begin_time")).hourOfDay().roundFloorCopy();
                DateTime lastTime = new DateTime(row.getAs("last_time")).hourOfDay().roundCeilingCopy();
                //每小时1笔数据
                while (begin_time.compareTo(lastTime) <= 0) {
                    rows.add(RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("base"), begin_time.getHourOfDay()));
                    begin_time = begin_time.plusHours(1);
                }
                return rows;
            }
        });

        DataFrame processedDf = sqlContext.createDataFrame(rdd, SCHEMA);
        DataFrame joinedDf = processedDf.join(workLiveDF, processedDf.col("msisdn").equalTo(workLiveDF.col("msisdn")), "left_outer")
                .select(
                        processedDf.col("date"),
                        processedDf.col("msisdn"),
                        processedDf.col("base"),
                        processedDf.col("time_inter"),
                        workLiveDF.col("person_class"),
                        workLiveDF.col("js_region"),
                        workLiveDF.col("sex"),
                        workLiveDF.col("age_class")
                );
        joinedDf = joinedDf.groupBy("date", "base", "time_inter", "person_class", "js_region", "sex", "age_class").agg(countDistinct("msisdn")
                .as("peo_num")).orderBy(col("date"),col("base"),col("time_inter"),col("person_class"),col("js_region"), col("sex"), col
                ("age_class"));
        FileUtil.saveFile(joinedDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/base-hour");
        return joinedDf;
    }
}
