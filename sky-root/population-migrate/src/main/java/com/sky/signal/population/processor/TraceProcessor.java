package com.sky.signal.population.processor;

import com.sky.signal.population.config.ParamProperties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by Chenhu on 2020/7/11.
 * 轨迹数据加载
 */
@Service
public class TraceProcessor implements Serializable {
    @Autowired
    private transient SQLContext sqlContext;
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
    private static final DateTimeFormatter FORMATTED = DateTimeFormat.forPattern("yyyyMMdd");
    @Autowired
    private transient ParamProperties params;
    /**
     * 加载指定路径所有的轨迹数据
     *
     * @param tracePathList 轨迹所在路径
     * @return
     */
    public DataFrame loadTrace(List<String> tracePathList) {
        DataFrame allTraceDf = null;

        for (String tracePath : tracePathList) {
            if (allTraceDf == null) {
                allTraceDf = sqlContext.read().parquet(tracePath);
            } else {
                allTraceDf = allTraceDf.unionAll(sqlContext.read().parquet
                        (tracePath));
            }
        }
        return allTraceDf;
    }

    /**
     * 通过区县基站信息找到属于当前区县的轨迹数据
     *
     * @param cell     当前区县基站信息
     * @param allTrace 全部的轨迹
     * @return
     */
    public DataFrame filterCurrentDistrictTrace(Broadcast<Map<String, Row>>
                                                        cell, DataFrame
            allTrace) {
        Map<String, Row> cellMap = cell.getValue();
        return allTrace.withColumn("line", lit("|")).filter(concat(col("lac")
                , column("line"), col("start_ci")).in(cellMap.keySet())).drop
                ("line");
    }

    /**
     * 过滤停留时间超过一定时间的手机号码的信令
     * @param traceDf
     * @return
     */
    public DataFrame filterStayMsisdnTrace(DataFrame traceDf,final String
            districtCode ) {
        return null;
    }

    /**
     * 把基站信息合并到轨迹中
     * @param traceDf
     * @param currentDistrictCell
     * @return
     */
    public DataFrame getDistrictDf(DataFrame traceDf, final Broadcast<Map<String, Row>>
            currentDistrictCell) {
        final Integer cityCode = params.getCityCode();
        JavaRDD<Row> rdd = traceDf.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String startTime = row.getAs("start_time").toString();
                String endTime = row.getAs("end_time").toString();
                Integer date = Integer.valueOf(DateTime.parse(startTime, FORMATTER).toString(FORMATTED));
                String msisdn = row.getAs("msisdn");
                Integer city_code = cityCode;
                Integer region = Integer.valueOf(row.getAs("reg_city").toString());
                Integer tac = Integer.valueOf(row.getAs("lac").toString());
                Long cell = Long.valueOf(row.getAs("start_ci").toString());
                String base = null;
                double lng = 0d;
                double lat = 0d;
                String district = null;
                Timestamp begin_time = new Timestamp(DateTime.parse(startTime, FORMATTER).getMillis());
                Timestamp end_time = new Timestamp(DateTime.parse(endTime, FORMATTER).getMillis());
                //根据tac/cell查找基站信息
                Row cellRow = currentDistrictCell.value().get(tac.toString() + '|' + cell.toString());
                if (cellRow == null) {
                    //Do nothing
                } else {
                    base = cellRow.getAs("base");
                    lng = cellRow.getAs("lng");
                    lat = cellRow.getAs("lat");
                    district = cellRow.getAs("district");
                }

                Row track = RowFactory.create(date, msisdn, region,
                        city_code,district, tac, cell, base, lng, lat, begin_time, end_time);
                return track;
            }
        });

        traceDf = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

        //过滤手机号码/基站不为0的数据,并删除重复手机信令数据
        DataFrame validDf = traceDf.filter(col("msisdn").notEqual("null").and(col("base").notEqual("null")).and(col("lng").notEqual(0)).and(col("lat")
                .notEqual(0))).dropDuplicates(new String[]{"msisdn", "begin_time"});
        return validDf;
    }
}
