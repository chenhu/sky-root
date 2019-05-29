package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.ProfileUtil;
import com.sky.signal.stat.util.TransformFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 15:57
 * description: 基站特定时间间隔,分目的出行特征统计,一般时段（0点-6点，9点-16点，19点-24点）以1小时为间隔，高峰时段（6点-9点，16点-19点）以15分钟为一个间隔
 */
@Service("oDTimeIntervalPurposeStat")
public class ODTimeIntervalPurposeStat implements Serializable{

    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame ODDf, SQLContext sqlContext) {

        JavaRDD<Row> odRDD = TransformFunction.transformTime(ODDf);
        ODDf = sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_TEMP_SCHEMA);


        ODDf = ODDf.groupBy("date", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"))
                .orderBy("date", "person_class", "trip_purpose", "sex", "age_class", "leaveTime_inter", "arriveTime_inter");

        FileUtil.saveFile(ODDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-time-interval-simple-stat");
        return ODDf;

    }
}
