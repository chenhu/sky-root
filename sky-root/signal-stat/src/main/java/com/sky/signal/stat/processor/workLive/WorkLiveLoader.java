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

import java.io.Serializable;

/**
 * 加载职住分析的数据
 */
@Service("workLiveLoader")
public class WorkLiveLoader implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveLoader.class);
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    ChangshuPersonClassification changshuPersonClassification;

    public DataFrame load(String workLiveFile) {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, workLiveFile);

        JavaRDD<Row> rdd = df.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String live_base = row.getAs("live_base");
                String work_base = row.getAs("work_base");
                // 归属地是否是江苏省，1: 是; 0: 否
                Integer region = row.getAs("region");
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
                Double stayTime = row.getAs("stay_time");
                if (stayTime == null) {
                    stayTime = 0d;
                }
                Integer stayTimeClass = TransformFunction.transformStayTimeClass(stayTime);

                Long existsDays = 0l;
                if(row.getAs("exists_days") !=null) {
                    existsDays = row.getAs("exists_days");
                }
                Long uld = 0l;
                Long uwd = 0l;
                Long on_lsd = 0l;
                Long on_wsd = 0l;
                if (live_base != null) {
                    if(row.getAs("uld") !=null) {
                        uld = row.getAs("uld");
                    }
                    if(row.getAs("on_lsd") !=null) {
                        on_lsd = row.getAs("on_lsd");
                    }
                }
                if (work_base != null) {
                    if(row.getAs("uwd") !=null) {
                        uwd = row.getAs("uwd");
                    }
                    if(row.getAs("on_wsd") !=null) {
                        on_wsd = row.getAs("on_wsd");
                    }
                }

                Double sum_time = row.getAs("stay_time");
                if(sum_time == null) {
                    sum_time = 0d;
                }
                //生成geohash
                Double work_lat = row.getAs("work_lat");
                Double work_lng = row.getAs("work_lng");
                GeoHash workGeoHash = new GeoHash(work_lat, work_lng);
                workGeoHash.sethashLength(7);
                String workGeo = workGeoHash.getGeoHashBase32();

                Double live_lat = row.getAs("live_lat");
                Double live_lng = row.getAs("live_lng");
                GeoHash liveGeoHash = new GeoHash(live_lat, live_lng);
                liveGeoHash.sethashLength(7);
                String liveGeo = liveGeoHash.getGeoHashBase32();


                Integer person_class = changshuPersonClassification.classify(existsDays, sum_time);
                return RowFactory.create(row.getAs("msisdn"), region, jsRegion, cenRegion, sex, row.getAs("age"), ageClass,
                        row.getAs("stay_time"), stayTimeClass, existsDays,live_base, liveGeo, row.getAs("live_lng"), row.getAs("live_lat"),
                        on_lsd, uld, work_base, workGeo, row.getAs("work_lng"), row.getAs("work_lat"), on_wsd, uwd, person_class);
            }

        });
        df = sqlContext.createDataFrame(rdd, LiveWorkSchemaProvider.WORK_LIVE_STAT_SCHEMA);
        return df;
    }
}
