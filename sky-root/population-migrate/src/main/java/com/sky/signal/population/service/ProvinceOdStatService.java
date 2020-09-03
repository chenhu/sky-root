package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.ODSchemaProvider;
import com.sky.signal.population.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DecimalType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Chenhu on 2020/7/11.
 * <pre>
 * 区县OD统计服务
 * 包括：
 * 对外出行OD 表
 * 对外出行OD(含出行人数)
 * 对外出行率统计表
 *
 * </pre>
 */
@Service
@Slf4j
public class ProvinceOdStatService implements ComputeService, Serializable {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;

    @Override
    public void compute() {
        //1.读取区县OD分析结果文件
        String odPath = params.getLimitedProvinceOdFilePath();
        DataFrame odDf = FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET, odPath).cache();

        //2. 生成相关字段的分类
        JavaRDD<Row> odRDD = odDf.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                int moveTimeClassic = transformMoveTimeClassic((Integer) row.getAs("move_time"));
                int durationOClassic = transformDurationODClassic((Integer) row.getAs("duration_o"));
                int durationDClassic = transformDurationODClassic((Integer) row.getAs("duration_d"));
                return RowFactory.create(row.getAs("date"),
                        row.getAs("msisdn"),
                        row.getAs("leave_city"),
                        row.getAs("leave_district"),
                        row.getAs("arrive_city"),
                        row.getAs("arrive_district"),
                        row.getAs("move_time"),
                        durationOClassic,
                        durationDClassic,
                        moveTimeClassic);
            }
        });

        DataFrame odClassicDf = sqlContext.createDataFrame(odRDD,ODSchemaProvider.OD_DISTRICT_SCHEMA_CLASSIC).cache();
        //20200902 增加moveTime为0的统计
        DataFrame odClassicDfMoveTimeNot0 = odClassicDf.filter(col("move_time").geq(60)).cache();
        //统计对外出行OD表1
        DataFrame table1 = odClassicDf.groupBy("date",
                "leave_city","leave_district","arrive_city","arrive_district","move_time_classic")
                .agg(sum("move_time").divide(60).cast(new DecimalType(10,1)).as("move_time_min"), count("msisdn").as("trip_num"))
                .select("date","leave_city","leave_district","arrive_city","arrive_district","move_time_classic","move_time_min","trip_num")
                .orderBy("date","leave_city","leave_district","arrive_city","arrive_district","move_time_classic");

        FileUtil.saveFile(table1.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table1"));
        DataFrame table1Temp = odClassicDfMoveTimeNot0.groupBy("date",
                "leave_city","leave_district","arrive_city","arrive_district","move_time_classic")
                .agg(sum("move_time").divide(60).cast(new DecimalType(10,1)).as("move_time_min"), count("msisdn").as("trip_num"))
                .select("date","leave_city","leave_district","arrive_city","arrive_district","move_time_classic","move_time_min","trip_num")
                .orderBy("date","leave_city","leave_district","arrive_city","arrive_district","move_time_classic");

        FileUtil.saveFile(table1Temp.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table1-1"));
        //统计对外出行OD2(含出行人数)
        DataFrame table2 = odClassicDf.groupBy("date",
                "leave_city","leave_district","arrive_city","arrive_district")
                .agg(count("msisdn").as("trip_num"), countDistinct("msisdn").as("peo_num"))
                .select("date","leave_city","leave_district","arrive_city","arrive_district","trip_num","peo_num")
                .orderBy("date","leave_city","leave_district","arrive_city","arrive_district");
        FileUtil.saveFile(table2.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table2"));
        DataFrame table2Temp = odClassicDfMoveTimeNot0.groupBy("date",
                "leave_city","leave_district","arrive_city","arrive_district")
                .agg(count("msisdn").as("trip_num"), countDistinct("msisdn").as("peo_num"))
                .select("date","leave_city","leave_district","arrive_city","arrive_district","trip_num","peo_num")
                .orderBy("date","leave_city","leave_district","arrive_city","arrive_district");
        FileUtil.saveFile(table2Temp.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table2-1"));
        //统计 对外出行率统计表
        //1. 统计地市出发地
        DataFrame city = odClassicDf.groupBy("date",
                "leave_city")
                .agg(count("msisdn").as("trip_num"), countDistinct("msisdn").as("peo_num"))
                .withColumnRenamed("leave_city","leave_o")
                .select("date","leave_o","trip_num","peo_num")
                .orderBy("date","leave_o");
        //2. 统计区县出发地
        DataFrame district = odClassicDf.groupBy("date",
                "leave_district")
                .agg(count("msisdn").as("trip_num"), countDistinct("msisdn").as("peo_num"))
                .withColumnRenamed("leave_district","leave_o")
                .select("date","leave_o","trip_num","peo_num")
                .orderBy("date","leave_o");

        //3. 合并地市和区县的统计结果
        DataFrame table3 = city.unionAll(district);
        FileUtil.saveFile(table3.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table3"));

        DataFrame cityTemp = odClassicDfMoveTimeNot0.groupBy("date",
                "leave_city")
                .agg(count("msisdn").as("trip_num"), countDistinct("msisdn").as("peo_num"))
                .withColumnRenamed("leave_city","leave_o")
                .select("date","leave_o","trip_num","peo_num")
                .orderBy("date","leave_o");
        //2. 统计区县出发地
        DataFrame districtTemp = odClassicDfMoveTimeNot0.groupBy("date",
                "leave_district")
                .agg(count("msisdn").as("trip_num"), countDistinct("msisdn").as("peo_num"))
                .withColumnRenamed("leave_district","leave_o")
                .select("date","leave_o","trip_num","peo_num")
                .orderBy("date","leave_o");

        //3. 合并地市和区县的统计结果
        DataFrame table3Temp = cityTemp.unionAll(districtTemp);
        FileUtil.saveFile(table3Temp.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table3-1"));

        odClassicDfMoveTimeNot0.unpersist();


        //最后一张表
        String durationNoneLimitedOdPath = params.getDestProvinceOdFilePath();
        DataFrame durationNoneLimitedOdDf = FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET, durationNoneLimitedOdPath).cache();
        JavaRDD<Row> durationLimitedRDD = durationNoneLimitedOdDf.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                int moveTimeClassic = transformMoveTimeClassic((Integer) row.getAs("move_time"));
                int durationOClassic = transformDurationODClassic((Integer) row.getAs("duration_o"));
                int durationDClassic = transformDurationODClassic((Integer) row.getAs("duration_d"));
                return RowFactory.create(row.getAs("date"),
                        row.getAs("msisdn"),
                        row.getAs("leave_city"),
                        row.getAs("leave_district"),
                        row.getAs("arrive_city"),
                        row.getAs("arrive_district"),
                        row.getAs("move_time"),
                        durationOClassic,
                        durationDClassic,
                        moveTimeClassic);
            }
        });

        DataFrame classicDf = sqlContext.createDataFrame(durationLimitedRDD,ODSchemaProvider.OD_DISTRICT_SCHEMA_CLASSIC).cache();
        DataFrame classicDfMoveTimeNot0 = classicDf.filter(col("move_time").geq(60));

        DataFrame table4 = classicDf.groupBy("date",
                "leave_city","leave_district","arrive_city","arrive_district","duration_o_classic","duration_d_classic","move_time_classic")
                .agg(sum("move_time").divide(60).cast(new DecimalType(10,1)).as("move_time_min"), count("msisdn").as("trip_num"))
                .select("date","leave_city","leave_district","arrive_city","arrive_district","duration_o_classic","duration_d_classic","move_time_classic","move_time_min","trip_num")
                .orderBy("date","leave_city","leave_district","arrive_city","arrive_district","duration_o_classic","duration_d_classic","move_time_classic");
        FileUtil.saveFile(table4.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table4"));
        DataFrame table4Temp = classicDfMoveTimeNot0.groupBy("date",
                "leave_city","leave_district","arrive_city","arrive_district","duration_o_classic","duration_d_classic","move_time_classic")
                .agg(sum("move_time").divide(60).cast(new DecimalType(10,1)).as("move_time_min"), count("msisdn").as("trip_num"))
                .select("date","leave_city","leave_district","arrive_city","arrive_district","duration_o_classic","duration_d_classic","move_time_classic","move_time_min","trip_num")
                .orderBy("date","leave_city","leave_district","arrive_city","arrive_district","duration_o_classic","duration_d_classic","move_time_classic");
        FileUtil.saveFile(table4Temp.repartition(1), FileUtil.FileType.CSV,params.getPopulationStatPath().concat("table4-1"));
    }

    private int transformMoveTimeClassic(Integer moveTime) {
        int moveTimeClassic;
        if (moveTime >= 0 && moveTime < 60 * 10) {
            moveTimeClassic = 1;
        } else if (moveTime >= 60 * 10 && moveTime < 60 * 30) {
            moveTimeClassic = 2;
        } else if (moveTime >= 60 * 30 && moveTime < 60 * 45) {
            moveTimeClassic = 3;
        } else if (moveTime >= 60 * 45 && moveTime < 60 * 60) {
            moveTimeClassic = 4;
        } else if (moveTime >= 60 * 60 && moveTime < 60 * 75) {
            moveTimeClassic = 5;
        } else if (moveTime >= 60 * 75 && moveTime < 60 * 90) {
            moveTimeClassic = 6;
        } else if (moveTime >= 60 * 90 && moveTime < 60 * 105) {
            moveTimeClassic = 7;
        } else if (moveTime >= 60 * 105 && moveTime < 60 * 120) {
            moveTimeClassic = 8;
        } else if (moveTime >= 60 * 120 && moveTime < 60 * 135) {
            moveTimeClassic = 9;
        } else if (moveTime >= 60 * 135 && moveTime < 60 * 150) {
            moveTimeClassic = 10;
        } else if (moveTime >= 60 * 150 && moveTime < 60 * 165) {
            moveTimeClassic = 11;
        } else if (moveTime >= 60 * 165 && moveTime < 60 * 180) {
            moveTimeClassic = 12;
        } else if (moveTime >= 60 * 180 && moveTime < 60 * 360) {
            moveTimeClassic = 13;
        } else {
            moveTimeClassic = 14;
        }
        return moveTimeClassic;
    }

    private int transformDurationODClassic(Integer moveTime) {
        int durationODClassic;
        if (moveTime >= 60 * 10 && moveTime < 60 * 40) {
            durationODClassic = 0;
        } else if (moveTime >= 60 * 40 && moveTime < 60 * 60) {
            durationODClassic = 1;
        } else if (moveTime >= 60 * 60 && moveTime < 60 * 90) {
            durationODClassic = 2;
        } else if (moveTime >= 60 * 90 && moveTime < 60 * 120) {
            durationODClassic = 3;
        } else if (moveTime >= 60 * 120 && moveTime < 60 * 150) {
            durationODClassic = 4;
        } else if (moveTime >= 60 * 150 && moveTime < 60 * 180) {
            durationODClassic = 5;
        } else if (moveTime >= 60 * 180 && moveTime < 60 * 210) {
            durationODClassic = 6;
        } else if (moveTime >= 60 * 210 && moveTime < 60 * 240) {
            durationODClassic = 7;
        } else if (moveTime >= 60 * 240 && moveTime < 60 * 270) {
            durationODClassic = 8;
        } else if (moveTime >= 60 * 270 && moveTime < 60 * 300) {
            durationODClassic = 9;
        } else if (moveTime >= 60 * 300 && moveTime < 60 * 720) {
            durationODClassic = 10;
        } else if (moveTime >= 60 * 720 && moveTime < 60 * 1080) {
            durationODClassic = 11;
        } else {
            durationODClassic = 12;
        }
        return durationODClassic;
    }
}
