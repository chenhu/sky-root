package com.sky.signal.pre.processor.transportationhub
        .StationPersonClassify;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.processor.transportationhub.PersonClassic;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Chenhu on 2020/5/27.
 * 昆山站人口分类器
 * 针对逗留时间小于60分钟的情况
 * <p>
 * 针对当天逗留时间 < 60min的用户（同一MSISDN），按照字段（start_time）排序，得到其轨迹
 * ，找出其中的枢纽点Pj，若Pj的start_time在0:00 – 0:35之间，且Pj的move_time >=
 * 3min，且在Pj之前（P1,
 * P2,…,Pj-1之中）有停留点或该用户在前一天有停留点，则判断该用户为铁路出发人口；若Pj的start_time在22:30 –
 * 24:00之间，且Pj的move_time >= 76s，且在Pj之后（Pj+1, …,
 * Pn之中）有停留点或该用户在后一天有停留点，则判断该用户为铁路到达人口；其他用户则判断为铁路过境人口
 */
@Component
public class KunShanStation implements Serializable {
    // 3分钟
    private static final Integer MOVE_TIME1 = 60 * 3;
    // 76秒
    private static final Integer MOVE_TIME2 = 76;
    DateTimeFormatter formatterDate = DateTimeFormat.forPattern
            ("yyyyMMdd");
    DateTimeFormatter formatterDateTime = DateTimeFormat.forPattern
            ("yyyy-MM-dd HH:mm:ss.S");
    DateTimeFormatter formatterDateTimeMi = DateTimeFormat
            .forPattern("yyyyMMddHHmm");
    @Autowired
    private transient ParamProperties params;

    public List<Row> classify(List<Row> rows, DataFrame
            stationTrace) {
        Ordering<Row> ordering = Ordering.natural().nullsFirst()
                .onResultOf
                        (new com.google.common.base.Function<Row,
                                Timestamp>() {
                            @Override
                            public Timestamp apply(Row row) {
                                return row.getAs("begin_time");
                            }
                        });
        //按startTime排序
        rows = ordering.sortedCopy(rows);
        rows = getClassic(rows, stationTrace);
        return rows;
    }

    private List<Row> getClassic(List<Row> rows, DataFrame
            stationTrace) {
        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());
        //默认为枢纽过境人口
        Integer personClassic = PersonClassic
                .TransportationHubPassBy.getIndex();

        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            Timestamp beginTime = row.getAs("begin_time");
            Integer date = row.getAs("date");
            Integer moveTime = row.getAs("move_time");

            //今天的日期
            LocalDate today = LocalDate.parse(date.toString(),
                    formatterDate);
            //昨天的日期
            Integer yesterday = Integer.valueOf(today.minusDays(1)
                    .toString("yyyyMMdd"));
            //明天的日期
            Integer tomorrow = Integer.valueOf(today.plusDays(1)
                    .toString("yyyyMMdd"));
            //手机号码
            String msisdn = row.getAs("msisdn");


            LocalDateTime localDateTime1 = LocalDateTime.parse(date
                    .toString() + "0000", formatterDateTimeMi);
            LocalDateTime localDateTime2 = LocalDateTime.parse(date
                    .toString() + "0035", formatterDateTimeMi);
            LocalDateTime localDateTime3 = LocalDateTime.parse(date
                    .toString() + "2230", formatterDateTimeMi);
            LocalDateTime localDateTime4 = LocalDateTime.parse(date
                    .toString() + "2359", formatterDateTimeMi);

            String base = row.getAs("base");
            // 如果是枢纽基站
            if (base.equals(params.getVisualStationBase())) {
                LocalDateTime beginTimeLocalDateTime =
                        LocalDateTime.parse(beginTime.toString(),
                                formatterDateTime);
                // 在00:00-00:35之间并且停留时间大于等于3分钟
                if (beginTimeLocalDateTime.isBefore(localDateTime2)
                        && beginTimeLocalDateTime.isAfter
                        (localDateTime1) && moveTime >= MOVE_TIME1) {
                    //判断在当前记录之前是否有停留点
                    Boolean hasStayPoint = false;
                    for (int j = 0; j < i; j++) {
                        byte tmpPointType = rows.get(j).getAs
                                ("point_type");
                        if (tmpPointType == SignalProcessUtil
                                .STAY_POINT) {
                            hasStayPoint = true;
                            break;
                        }
                    }
                    if (hasStayPoint) { // 枢纽站之前有停留点，则为枢纽站出发人口
                        personClassic = PersonClassic.Leave
                                .getIndex();
                        //判断出最终结果，结束循环
                        break;
                    } else {//判断昨天是否有停留点
                        DataFrame yesterDayDf = stationTrace.filter
                                (col("date").equalTo(yesterday))
                                .filter(col("msisdn").equalTo
                                        (msisdn)).filter
                                        (col("point_type").equalTo
                                                (SignalProcessUtil
                                                        .STAY_POINT));
                        //昨天基站轨迹信令中有停留点,结束循环
                        if (yesterDayDf.count() > 0L) {
                            personClassic = PersonClassic.Leave
                                    .getIndex();
                            break;

                        }

                    }
                } else if (beginTimeLocalDateTime.isBefore
                        (localDateTime4) && beginTimeLocalDateTime
                        .isAfter(localDateTime3) && moveTime >=
                        MOVE_TIME2) {//在22:30到23:59分之间并且停留时间大于等于76秒
                    //判断在当前记录之后是否有停留点
                    Boolean hasStayPoint = false;
                    for (int j = i + 1; j < rows.size(); j++) {
                        byte tmpPointType = rows.get(j).getAs
                                ("point_type");
                        if (tmpPointType == SignalProcessUtil
                                .STAY_POINT) {
                            hasStayPoint = true;
                            break;
                        }
                    }
                    if (hasStayPoint) { // 枢纽站之前有停留点，则为枢纽站出发人口
                        personClassic = PersonClassic.Arrive
                                .getIndex();
                        //判断出最终结果，结束循环
                        break;
                    } else {//判断明天是否有停留点
                        DataFrame tomorrowDayDf = stationTrace.filter
                                (col("date").equalTo(tomorrow))
                                .filter(col("msisdn").equalTo
                                        (msisdn)).filter
                                        (col("point_type").equalTo
                                                (SignalProcessUtil
                                                        .STAY_POINT));
                        //明天基站轨迹信令中有停留点,结束循环
                        if (tomorrowDayDf.count() > 0) {
                            personClassic = PersonClassic.Arrive
                                    .getIndex();
                            break;

                        }
                    }

                }

            }
        }

        //根据人口分类，创建带人口分类的信令数据集合
        for (Row row : rows) {
            Row rowWithPersionClassic = new GenericRowWithSchema(new
                    Object[]{row.getAs("date"), row.getAs
                    ("msisdn"), row.getAs("base"),
                    row.getAs("lng"), row.getAs("lat"),
                    row.getAs("begin_time"), row.getAs
                    ("last_time"), row.getAs("distance"), row
                    .getAs("move_time"), row.getAs("speed"), row
                    .getAs("point_type"), personClassic.byteValue()},
                    ODSchemaProvider.STATION_TRACE_CLASSIC_SCHEMA);
            resultList.add(rowWithPersionClassic);
        }

        return resultList;
    }
}
