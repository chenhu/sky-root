package com.sky.signal.pre.processor.transportationhub.StationPersonClassify;

import com.google.common.collect.Ordering;
import org.apache.spark.sql.Row;
import org.joda.time.LocalTime;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/27.
 * 昆山站人口分类器
 * 针对逗留时间小于60分钟的情况
 * <p>
 * 针对当天逗留时间 < 60min的用户（同一MSISDN），按照字段（start_time）排序，得到其轨迹
 * ，找出其中的枢纽点Pj，若Pj的start_time在0:00 – 0:35之间，且Pj的move_time >= 3min，且在Pj之前（P1,
 * P2,…,Pj-1之中）有停留点或该用户在前一天有停留点，则判断该用户为铁路出发人口；若Pj的start_time在22:30 –
 * 24:00之间，且Pj的move_time >= 76s，且在Pj之后（Pj+1, …,
 * Pn之中）有停留点或该用户在后一天有停留点，则判断该用户为铁路到达人口；其他用户则判断为铁路过境人口
 */
@Component
public class KunShanStation implements Station, Serializable {
    private static final LocalTime time0000 = new LocalTime(0, 0);
    private static final LocalTime time0035 = new LocalTime(0, 35);
    private static final LocalTime time2230 = new LocalTime(22, 30);
    private static final LocalTime time2359 = new LocalTime(23, 59);

    @Override
    public List<Row> classify(List<Row> rows) {
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("begin_time");
            }
        });
        //按startTime排序
        rows = ordering.sortedCopy(rows);
        return null;
    }
}
