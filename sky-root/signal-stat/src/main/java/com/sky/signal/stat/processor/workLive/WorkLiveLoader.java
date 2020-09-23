package com.sky.signal.stat.processor.workLive;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;

/**
 * 加载职住分析的数据
 */
@Service("workLiveLoader")
public class WorkLiveLoader implements Serializable {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;

    public DataFrame load(String workLiveFile) {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.PARQUET, LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, workLiveFile);
        JavaRDD<Row> rdd = df.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String live_base = (String) row.getAs("live_base");
                String work_base = (String) row.getAs("work_base");
                // 归属地是否是江苏省，1: 是; 0: 否
                Integer region = (Integer) row.getAs("region");
                if (region == null) {
                    region = 0;
                }
                Integer jsRegion = TransformFunction.transformJsRegion(region);
                // 性别
                Short sex = TransformFunction.transformSexClass((Short) row.getAs("sex"), region);
                // 年龄分类
                Integer ageClass = TransformFunction.transformAgeClass((Short) row.getAs("age"), region);

                String cenRegion = TransformFunction.transformCenRegion((Integer) row.getAs("cen_region"));
                // 分析时间范围内每日平均逗留时间分类
                Double stayTime = (Double) row.getAs("stay_time");
                if (stayTime == null) {
                    stayTime = 0d;
                }
                Integer stayTimeClass = TransformFunction.transformStayTimeClass(stayTime);

                Long existsDays = 0l;
                if (row.getAs("exists_days") != null) {
                    existsDays = (Long) row.getAs("exists_days");
                }
                Long uld = 0l;
                Long uwd = 0l;
                Long on_lsd = 0l;
                Long on_wsd = 0l;
                if (live_base != null) {
                    if (row.getAs("uld") != null) {
                        uld = (Long) row.getAs("uld");
                    }
                    if (row.getAs("on_lsd") != null) {
                        on_lsd = (Long) row.getAs("on_lsd");
                    }
                }
                if (work_base != null) {
                    if (row.getAs("uwd") != null) {
                        uwd = (Long) row.getAs("uwd");
                    }
                    if (row.getAs("on_wsd") != null) {
                        on_wsd = (Long) row.getAs("on_wsd");
                    }
                }

                Double sum_time = (Double) row.getAs("stay_time");
                if (sum_time == null) {
                    sum_time = 0d;
                }
                //生成geohash
                Double work_lat = (Double) row.getAs("work_lat");
                Double work_lng = (Double) row.getAs("work_lng");
                String workGeo = GeoUtil.geo(work_lat, work_lng);

                Double live_lat = (Double) row.getAs("live_lat");
                Double live_lng = (Double) row.getAs("live_lng");
                String liveGeo = GeoUtil.geo(live_lat, live_lng);

                Integer person_class = ChangshuPersonClassification.classify(existsDays, sum_time);

                return RowFactory.create(row.getAs("msisdn"),
                        region, jsRegion, cenRegion, sex, row.getAs("age"), ageClass,
                        row.getAs("stay_time"), stayTimeClass, existsDays, live_base, liveGeo,
                        row.getAs("live_lng"), row.getAs("live_lat"), on_lsd, uld,
                        work_base, workGeo, row.getAs("work_lng"), row.getAs("work_lat"), on_wsd, uwd, person_class);
            }

        });


        DataFrame workLiveDf = sqlContext.createDataFrame(rdd, LiveWorkSchemaProvider.WORK_LIVE_STAT_SCHEMA);
        // 如果当前分析的日期不在主职住日期内，应该把当前日期的职住信息和主职住信息合并，方法为：如果某人在主职住有信息，则用主职住，如果没有，采用当前分析日期的职住
        if (!StringUtils.isEmpty(params.getOtherWorkLiveName())) {
            DataFrame workLiveDf1 = FileUtil.readFile(FileUtil.FileType.PARQUET,
                    LiveWorkSchemaProvider.WORK_LIVE_SCHEMA,
                    params.getWorkLiveFileByName(params.getOtherWorkLiveName()))
                    .select("msisdn", "work_base", "live_base", "exists_days", "stay_time", "live_lng", "live_lat", "work_lng", "work_lat")
                    .withColumnRenamed("msisdn", "msisdn1")
                    .withColumnRenamed("work_base", "work_base1")
                    .withColumnRenamed("live_base", "live_base1")
                    .withColumnRenamed("exists_days", "exists_days1")
                    .withColumnRenamed("stay_time", "stay_time1")
                    .withColumnRenamed("live_lng", "live_lng1")
                    .withColumnRenamed("live_lat", "live_lat1")
                    .withColumnRenamed("work_lng", "work_lng1")
                    .withColumnRenamed("work_lat", "work_lat1");

            //左连接
            DataFrame joinedDf = workLiveDf.join(workLiveDf1, workLiveDf.col("msisdn").equalTo(workLiveDf1.col("msisdn1")), "left_outer");
            JavaRDD<Row> workLiveRdd = joinedDf.javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {

                    String workBase = (String) row.getAs("work_base");
                    String liveBase = (String) row.getAs("live_base");
                    //生成geohash
                    Double work_lat = (Double) row.getAs("work_lat");
                    Double work_lng = (Double) row.getAs("work_lng");
                    String workGeo = GeoUtil.geo(work_lat, work_lng);

                    Double live_lat = (Double) row.getAs("live_lat");
                    Double live_lng = (Double) row.getAs("live_lng");
                    String liveGeo = GeoUtil.geo(live_lat, live_lng);
                    //默认为访客人口
                    Integer personClass = 2;

                    //如果在第二份中有职住信息，就按照第二份中的职住地，并且人口分类也按照第二份
                    if (row.getAs("work_base1") != null && row.getAs("live_base1") != null) {

                        workBase = (String) row.getAs("work_base1");
                        liveBase = (String) row.getAs("live_base1");

                        Double sum_time = (Double) row.getAs("stay_time1");
                        if (sum_time == null) {
                            sum_time = 0d;
                        }
                        Long existsDays = 0l;
                        if (row.getAs("exists_days1") != null) {
                            existsDays = (Long) row.getAs("exists_days1");
                        }
                        personClass = ChangshuPersonClassification.classify(existsDays, sum_time);

                        work_lat = (Double) row.getAs("work_lat1");
                        work_lng = (Double) row.getAs("work_lng1");
                        workGeo = GeoUtil.geo(work_lat, work_lng);

                        live_lat = (Double) row.getAs("live_lat1");
                        live_lng = (Double) row.getAs("live_lng1");
                        liveGeo = GeoUtil.geo(live_lat, live_lng);

                    }
                    return RowFactory.create(row.getAs("msisdn"), row.getAs("region"), row.getAs("js_region"), row.getAs("cen_region"), row.getAs("sex"), row.getAs("age"), row.getAs("age_class"), row.getAs("stay_time"), row.getAs("stay_time_class"), row.getAs("exists_days"), liveBase, liveGeo, live_lng, live_lat, row.getAs("on_lsd"), row.getAs("uld"), workBase, workGeo, work_lng, work_lat, row.getAs("on_wsd"), row.getAs("uwd"), personClass);
                }
            });
            workLiveDf = sqlContext.createDataFrame(workLiveRdd, LiveWorkSchemaProvider.WORK_LIVE_STAT_SCHEMA);
        }
        return workLiveDf;
    }
}
