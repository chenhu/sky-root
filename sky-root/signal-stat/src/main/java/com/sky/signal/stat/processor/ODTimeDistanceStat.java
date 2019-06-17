package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.TransformFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DecimalType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 15:57
 * description: 出行时耗-距离分布
 */
@Service("oDTimeDistanceStat")
public class ODTimeDistanceStat implements Serializable{

    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame ODDf, SQLContext sqlContext) {
        JavaRDD<Row> odRDD = TransformFunction.transformTimeDistanceSpeed(ODDf);

        ODDf = sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_STAT_TEMP_SCHEMA);


        ODDf = ODDf.groupBy("date", "person_class", "trip_purpose", "move_time_class", "distance_class", "max_speed_class", "cov_speed_class")
                .agg(sum("move_time").divide(60).cast(new DecimalType(10,2)).as("sum_time"),
                        sum("linked_distance").divide(1000).cast(new DecimalType(10,2)).as("sum_dis"),
                        count("*").as("trip_num"),
                        countDistinct("msisdn").as("num_inter"))
                .orderBy("date", "person_class", "trip_purpose", "move_time_class", "distance_class", "max_speed_class", "cov_speed_class");

        FileUtil.saveFile(ODDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-time-distance-stat");
        return ODDf;

    }
}
