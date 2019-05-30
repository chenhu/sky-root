package com.sky.signal.stat.processor;

import com.sky.signal.stat.processor.od.ODSchemaProvider;
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

    @Autowired
    private transient SQLContext sqlContext;
    public DataFrame process(DataFrame od, DataFrame workLiveStat) {
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

                Integer leaveHomeDis = 10000;
                if (liveLng != null && liveLat != null) {
                    leaveHomeDis = MapUtil.getDistance(leaveLng, leaveLat, liveLng, liveLat);
                }

                Integer leaveWorkDis = 10000;
                if (workLng != null && workLat != null) {
                    leaveWorkDis = MapUtil.getDistance(leaveLng, leaveLat, workLng, workLat);
                }

                Integer arriveHomeDis = 10000;
                if (liveLng != null && liveLat != null) {
                    arriveHomeDis = MapUtil.getDistance(arriveLng, arriveLat, liveLng, liveLat);
                }
                Integer arriveWorkDis = 10000;
                if (workLng != null && workLat != null) {
                    arriveWorkDis = MapUtil.getDistance(arriveLng, arriveLat, workLng, workLat);
                }

                String liveBase = row.getAs("live_base");
                if (leaveHomeDis <= 800) {
                    liveBase = row.getAs("leave_base");
                } else if (arriveHomeDis <= 800) {
                    liveBase = row.getAs("arrive_base");
                }
                String workBase = row.getAs("work_base");
                if (leaveWorkDis <= 800) {
                    workBase = row.getAs("leave_base");
                } else if (arriveWorkDis <= 800) {
                    workBase = row.getAs("arrive_base");
                }

                String live_base = row.getAs("live_base");
                String work_base = row.getAs("work_base");
                String leave_base = row.getAs("leave_base");
                String arrive_base = row.getAs("arrive_base");
                Short trip_purpose;

                if ((leave_base.equals(live_base) && arrive_base.equals(work_base))) {
                    trip_purpose = 1;// HBWPA 通勤出行，从家到工作地
                } else if (leave_base.equals(work_base) && arrive_base.equals(live_base)) {
                    trip_purpose = 2;// HBWAP 通勤出行，从工作地到家
                } else if (leave_base.equals(live_base) && !arrive_base.equals(work_base)) {
                    trip_purpose = 3;// HBOAP 出行起点为家，终点为其他（非工作地非学校）
                } else if (arrive_base.equals(live_base) && !leave_base.equals(work_base)) {
                    trip_purpose = 4;// HBOPA 出行起点为其他（非工作地非学校），终点为家
                } else if (!leave_base.equals(live_base) && !arrive_base.equals(work_base)) {
                    trip_purpose = 5;// NHB 出行起点为其他（非家非工作地非学校），终点为其他（非家非工作地非学校）
                } else {
                    trip_purpose = 6;// 其他
                }
                return RowFactory.create(new Object[]{row.getAs("date"), row.getAs("msisdn"), row.getAs("leave_base"), row.getAs("arrive_base"), row.getAs("leave_time"),
                        row.getAs("arrive_time"), row.getAs("linked_distance"), row.getAs("max_speed"), row.getAs("cov_speed"), row.getAs("distance"), row.getAs("move_time"), row.getAs("age_class"),
                        row.getAs("sex"), row.getAs("person_class"), liveBase, workBase, trip_purpose});
            }
        });
        return sqlContext.createDataFrame(joinedRDD, ODSchemaProvider.OD_STAT_SCHEMA);
    }

}
