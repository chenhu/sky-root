package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.TransformFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 15:57
 * description: 基站特定时间间隔OD表,
 * 1. 一天时间分时段分析，不分目的 一般时段（0点-6点，9点-16点，19点-24点）以1小时为间隔，高峰时段（6点-9点，16点-19点）以15分钟为一个间隔
 * 2. 一天时段不分时段，不分目的 全天都是一小时为一个间隔
 * 3. 一天时间分时段分析，分目的 一般时段（0点-6点，9点-16点，19点-24点）以1小时为间隔，高峰时段（6点-9点，16点-19点）以15分钟为一个间隔
 * 4. 一天时段不分时段，分目的 全天都是一小时为一个间隔
 */
@Service("oDTimeIntervalStat")
public class ODTimeIntervalStat implements Serializable{

    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame oDDf, SQLContext sqlContext) {
        oDDf.persist(StorageLevel.DISK_ONLY());
        // 特殊分析，高峰和平峰时段分析
        JavaRDD<Row> odRDDSpecial = TransformFunction.transformTime(oDDf);
        // 不分时段分析
        JavaRDD<Row> odRDDGeneral = TransformFunction.transformTime1(oDDf);

        DataFrame specialDf = sqlContext.createDataFrame(odRDDSpecial, ODSchemaProvider.OD_TEMP_SCHEMA);
        DataFrame generalDf = sqlContext.createDataFrame(odRDDGeneral, ODSchemaProvider.OD_TEMP_SCHEMA);


        DataFrame specialDf1 = specialDf.groupBy("date", "leave_base", "arrive_base", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"), sum("move_time").as("sum_time"), sum("distance").as("sum_dis"))
                .withColumn("avg_time",floor(col("sum_time").divide(col("trip_num")).divide(60)))
                .withColumn("avg_dis", floor(col("sum_dis").divide(col("trip_num")))).drop("sum_time").drop("sum_dis")
                .orderBy("date","leave_base", "arrive_base", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter");

        FileUtil.saveFile(specialDf1.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-time-interval-special-stat");

        DataFrame generalDf1 = generalDf.groupBy("date", "leave_base", "arrive_base", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"), sum("move_time").as("sum_time"), sum("distance").as("sum_dis"))
                .withColumn("avg_time",floor(col("sum_time").divide(col("trip_num")).divide(60)))
                .withColumn("avg_dis", floor(col("sum_dis").divide(col("trip_num")))).drop("sum_time").drop("sum_dis")
                .orderBy("date","leave_base", "arrive_base", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter");

        FileUtil.saveFile(generalDf1.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-time-interval-general-stat");


        DataFrame purposeSpecialDf = specialDf.groupBy("date", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"))
                .orderBy("date", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter");

        FileUtil.saveFile(purposeSpecialDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-time-interval-purpose-special-stat");

        DataFrame purposeGeneralDf = generalDf.groupBy("date", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"))
                .orderBy("date", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter");

        FileUtil.saveFile(purposeGeneralDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-time-interval-purpose-general-stat");
        oDDf.unpersist();
        return null;
    }
}
