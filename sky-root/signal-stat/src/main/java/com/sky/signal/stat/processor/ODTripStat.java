package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.countDistinct;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/6/16 15:57
 * description: 出行次数统计
 */
@Service("oDTripStat")
public class ODTripStat implements Serializable{
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    public DataFrame process(DataFrame odTripStat) {
        JavaRDD<Row> odTripStatRDD = odTripStat.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                Integer tripClass = 0;
                Byte hasTrip = row.getAs("has_trip");
                Integer staypointCount = row.getAs("staypoint_count");
                if(hasTrip == 0) {
                    if(staypointCount == 0) {
                        tripClass = 3;
                    } else if(staypointCount == 1) {
                        tripClass = 1;
                    } else {
                        tripClass = 2;
                    }
                }

                return new GenericRowWithSchema(new Object[]{row.getAs("date"), row.getAs("msisdn"),row.getAs("person_class"),tripClass}, ODSchemaProvider.OD_TRIP_STAT_SCHEMA2);
            }
        });
        odTripStat = sqlContext.createDataFrame(odTripStatRDD, ODSchemaProvider.OD_TRIP_STAT_SCHEMA2);
        DataFrame df = odTripStat.groupBy("date", "person_class", "trip_class")
                .agg(countDistinct("msisdn").as("peo_num"))
                .orderBy("date","person_class", "trip_class");
        FileUtil.saveFile(df.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/od-trip-class-stat");
        return df;

    }
}
