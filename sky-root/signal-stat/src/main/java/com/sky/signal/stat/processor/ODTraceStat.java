package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 15:57
 * description: OD出行中间点的相关统计
 */
@Service("odTraceStat")
public class ODTraceStat implements Serializable{

    @Autowired
    private transient ParamProperties params;

    private static final LocalTime time0900 = new LocalTime(9, 0);
    private static final LocalTime time0800 = new LocalTime(8, 0);
    private static final LocalTime time0700 = new LocalTime(7, 0);

    public DataFrame process(DataFrame ODTraceDf, DataFrame workLiveDf, Integer batchId, SQLContext sqlContext) {
        DataFrame replaced = ODTraceDf.join(workLiveDf, ODTraceDf.col("msisdn").equalTo(workLiveDf.col("msisdn")), "left_outer");
        replaced = replaced.select(
                ODTraceDf.col("date"),
                ODTraceDf.col("msisdn"),
                ODTraceDf.col("leave_base"),
                ODTraceDf.col("arrive_base"),
                ODTraceDf.col("leave_time"),
                ODTraceDf.col("arrive_time"),
                workLiveDf.col("age_class"),
                workLiveDf.col("sex"),
                workLiveDf.col("person_class"));
        replaced.persist(StorageLevel.DISK_ONLY());

        DataFrame odDayTraceDf = replaced.groupBy("date", "person_class", "sex", "age_class", "leave_base", "arrive_base")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"))
                .orderBy("date", "person_class", "sex", "age_class", "leave_base", "arrive_base");

        FileUtil.saveFile(odDayTraceDf, FileUtil.FileType.PARQUET, params.getODTraceDaySavePath(batchId.toString()));
        JavaRDD<Row> traceRdd = replaced.toJavaRDD();
        traceRdd = traceRdd.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                DateTime leave_time = new DateTime(row.getAs("leave_time"));
                DateTime arrive_time = new DateTime(row.getAs("arrive_time"));
                LocalTime time1 = leave_time.toLocalTime();
                LocalTime time2 = arrive_time.toLocalTime();
                return time1.compareTo(time0700) > 0 && time1.compareTo(time0900) <= 0 && time2.compareTo(time0700) > 0 && time2.compareTo(time0900) <= 0;
            }
        });
        traceRdd = traceRdd.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                DateTime leave_time = new DateTime(row.getAs("leave_time"));
                DateTime arrive_time = new DateTime(row.getAs("arrive_time"));
                LocalTime time1 = leave_time.toLocalTime();
                LocalTime time2 = arrive_time.toLocalTime();
                Integer fromTimeClass , toTimeClass;
                if(time1.compareTo(time0700) > 0 && time1.compareTo(time0800)<= 0)
                {
                    fromTimeClass = 1;
                } else {
                    fromTimeClass = 2;
                }
                if(time2.compareTo(time0700) > 0 && time2.compareTo(time0800)<= 0)
                {
                    toTimeClass = 1;
                } else {
                    toTimeClass = 2;
                }
                return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("person_class"),
                        row.getAs("sex"), row.getAs("age_class"), row.getAs("leave_base"), row.getAs("arrive_base"), fromTimeClass, toTimeClass);
            }
        });

        DataFrame newTraceDf = sqlContext.createDataFrame(traceRdd, ODSchemaProvider.OD_TRACE_STAT_SCHEMA);
        newTraceDf = newTraceDf.groupBy("date", "person_class", "sex", "age_class", "leave_base", "arrive_base", "from_time_class",  "arrive_time_class" )
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"))
                .orderBy("date", "person_class", "sex", "age_class", "leave_base", "arrive_base", "from_time_class", "arrive_time_class");

        FileUtil.saveFile(newTraceDf, FileUtil.FileType.PARQUET, params.getODTraceBusyTimeSavePath(batchId.toString()));
        replaced.unpersist();
        return null;

    }

    public void combineData() {
        DataFrame odTraceDayDf =  FileUtil.readFile(FileUtil.FileType.PARQUET, ODSchemaProvider.OD_TRACE_STAT_SCHEMA1,params.getODTraceDaySavePath("*"));
        FileUtil.saveFile(odTraceDayDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getODTraceDaySavePath());
        DataFrame odTraceBusyTimeDf =  FileUtil.readFile(FileUtil.FileType.PARQUET, ODSchemaProvider.OD_TRACE_STAT_SCHEMA1,params.getODTraceBusyTimeSavePath("*"));
        FileUtil.saveFile(odTraceBusyTimeDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getODTraceBusyTimeSavePath());
    }
}
