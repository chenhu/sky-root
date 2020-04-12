package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.GeoHash;
import com.sky.signal.stat.util.MapUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/29 22:07
 * description: 合并OD数据和职住数据，判断OD出行的目的
 */
@Service("combineODWorkLive")
public class CombineODWorkLive implements Serializable {

    // 尝试 800,1000,1200,1500出行率的差别
    private static final int CONST_LEAVE = 1000;
    private static final int CONST_LARGE = 10000;

    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    public DataFrame process(DataFrame od, DataFrame workLiveStat, Integer batchId) {
        DataFrame replaced = od.join(workLiveStat, od.col("msisdn").equalTo(workLiveStat.col("msisdn")), "left_outer");
        replaced = replaced.select(
                od.col("date"),
                od.col("msisdn"),
                od.col("leave_base"),
                od.col("leave_lng"),
                od.col("leave_lat"),
                od.col("arrive_base"),
                od.col("arrive_lng"),
                od.col("arrive_lat"),
                od.col("leave_time"),
                od.col("arrive_time"),
                od.col("linked_distance"),
                od.col("max_speed"),
                od.col("cov_speed"),
                od.col("distance"),
                od.col("move_time"),
                workLiveStat.col("age_class"),
                workLiveStat.col("sex"),
                workLiveStat.col("person_class"),
                workLiveStat.col("live_base"),
                workLiveStat.col("live_lng"),
                workLiveStat.col("live_lat"),
                workLiveStat.col("work_base"),
                workLiveStat.col("work_lng"),
                workLiveStat.col("work_lat"));

        JavaRDD<Row> joinedRDD = replaced.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                Double leaveLng = row.getAs("leave_lng");
                Double leaveLat = row.getAs("leave_lat");
                Double arriveLng = row.getAs("arrive_lng");
                Double arriveLat = row.getAs("arrive_lat");
                Double liveLng = row.getAs("live_lng");
                Double liveLat = row.getAs("live_lat");
                Double workLng = row.getAs("work_lng");
                Double workLat = row.getAs("work_lat");
                //计算出发基站和到达基站的geohash
                GeoHash leaveGeoHash = new GeoHash(leaveLat, leaveLng);
                leaveGeoHash.sethashLength(7);
                String leaveGeo = leaveGeoHash.getGeoHashBase32();

                GeoHash arriveGeoHash = new GeoHash(arriveLat, arriveLng);
                arriveGeoHash.sethashLength(7);
                String arriveGeo = arriveGeoHash.getGeoHashBase32();

                // 10000米的设置，是为了设置一个尽量大的值，在没有判断出职住基站的情况下，让出发或者到达地点尽量离家或工作地远些
                Integer leaveHomeDis = CONST_LARGE;
                if (liveLng != null && liveLat != null) {
                    leaveHomeDis = MapUtil.getDistance(leaveLng, leaveLat, liveLng, liveLat);
                }

                Integer leaveWorkDis = CONST_LARGE;
                if (workLng != null && workLat != null) {
                    leaveWorkDis = MapUtil.getDistance(leaveLng, leaveLat, workLng, workLat);
                }

                Integer arriveHomeDis = CONST_LARGE;
                if (liveLng != null && liveLat != null) {
                    arriveHomeDis = MapUtil.getDistance(arriveLng, arriveLat, liveLng, liveLat);
                }
                Integer arriveWorkDis = CONST_LARGE;
                if (workLng != null && workLat != null) {
                    arriveWorkDis = MapUtil.getDistance(arriveLng, arriveLat, workLng, workLat);
                }

                // 如果离开位置离家或工作地小于一个比较合理的常量，比如说800米，就认为是从家或者从工作地出发的。因为信令信号会在附近基站切换

                String liveBase = row.getAs("live_base");
                if (leaveHomeDis <= CONST_LEAVE) {
                    liveBase = row.getAs("leave_base");
                } else if (arriveHomeDis <= CONST_LEAVE) {
                    liveBase = row.getAs("arrive_base");
                }
                String workBase = row.getAs("work_base");
                if (leaveWorkDis <= CONST_LEAVE) {
                    workBase = row.getAs("leave_base");
                } else if (arriveWorkDis <= CONST_LEAVE) {
                    workBase = row.getAs("arrive_base");
                }
                String leave_base = row.getAs("leave_base");
                String arrive_base = row.getAs("arrive_base");
                Short trip_purpose;

                if ((leave_base.equals(liveBase) && arrive_base.equals(workBase))) {
                    trip_purpose = 1;// HBWPA 通勤出行，从家到工作地
                } else if (leave_base.equals(workBase) && arrive_base.equals(liveBase)) {
                    trip_purpose = 2;// HBWAP 通勤出行，从工作地到家
                } else if (leave_base.equals(liveBase) && !arrive_base.equals(workBase)) {
                    trip_purpose = 3;// HBOAP 出行起点为家，终点为其他（非工作地非学校）
                } else if (!leave_base.equals(workBase) && arrive_base.equals(liveBase)) {
                    trip_purpose = 4;// HBOPA 出行起点为其他（非工作地非学校），终点为家
                } else if (!leave_base.equals(liveBase) && !arrive_base.equals(liveBase)) {
                    trip_purpose = 5;// NHB 出行起点为非家，终点为非家
                } else {
                    trip_purpose = 6;// 其他
                }
                return RowFactory.create(new Object[]{row.getAs("date"), row.getAs("msisdn"), row.getAs("leave_base"),leaveGeo, row.getAs("arrive_base"), arriveGeo, row.getAs("leave_time"),
                        row.getAs("arrive_time"), row.getAs("linked_distance"), row.getAs("max_speed"), row.getAs("cov_speed"), row.getAs("distance"), row.getAs("move_time"), row.getAs("age_class"),
                        row.getAs("sex"), row.getAs("person_class"), liveBase, row.getAs("live_geo"), workBase, row.getAs("work_geo"), trip_purpose});
            }
        });
        DataFrame joinedDf = sqlContext.createDataFrame(joinedRDD, ODSchemaProvider.OD_STAT_SCHEMA);

        FileUtil.saveFile(joinedDf, FileUtil.FileType.CSV, params.getSavePath() + "stat/combine-batch/"+batchId+"/combine-od");

        return joinedDf;
    }

    public DataFrame read() {
        return FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_STAT_SCHEMA,params.getSavePath() + "stat/combine-batch/*/combine-od").repartition(params.getPartitions());
    }
}
