package com.sky.signal.pre.processor.transportationhub;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/25.
 * 枢纽站人口分类器
 */
@Component
public class StationPersonClassification implements Serializable {

    public void process(DataFrame stationValidSignal) {
        // 停留点类型判定
        DataFrame stayPointValidSignal = stayPointType(stationValidSignal);

    }

    /**
     * 确定停留点类型
     * 针对每一条手机数据，计算当前位置点与后一位置点的时间差t及空间点位移速度v; 若该数据所占用的基站是枢纽站，t >= 10min
     * 则判断当前位置为停留点，t < 10min 则为位移点；若当前数据为非枢纽站数据，t >= 40min则当前位置点为确定停留点,若10min
     * <= t < 40min 且 v < 8km/h 则为可能停留点，否则为位移点。（0=确定停留点，1=可能停留点，2=位移点）
     *
     * @param df 待分类的有效信令
     * @return
     */
    private DataFrame stayPointType(DataFrame df) {
        return null;
    }

    /**
     * 人口分类
     * 针对每个用户（以MSISDN字段为区分），对move_time进行求和，计算其当天全部的逗留时间；对于逗留时间 >=
     * 60min的用户，直接进入第⑤步，对于逗留时间 < 60min的用户，直接进入步骤④进行铁路过境人口判断
     *
     * @param df 待分类的有效信令
     * @return
     */
    private DataFrame personClassification(DataFrame df) {
        return null;
    }

    /**
     * 铁路过境人口分类
     * 针对当天逗留时间 < 60min的用户（同一MSISDN），按照字段（start_time）排序，得到其轨迹
     * ，找出其中的枢纽点Pj，若Pj的start_time在0:00 – 0:35之间，且Pj的move_time >=
     * 3min，且在Pj之前（P1,P2,…,Pj-1之中）有停留点或该用户在前一天有停留点，则判断该用户为铁路出发人口；若Pj
     * 的start_time在22:30 – 24:00之间，且Pj的move_time >= 76s，且在Pj之后（Pj+1, …,
     * Pn之中）有停留点或该用户在后一天有停留点，则判断该用户为铁路到达人口；其他用户则判断为铁路过境人口。
     *
     * @param rows 用户一条的信令
     * @return 增加分类后的用户数据
     */
    private List<Row> railwayPassPerson(List<Row> rows) {
        return null;
    }

    /**
     * 枢纽站人口分类
     *
     * @param rows 用户一条的信令
     * @return 增加分类后的用户数据
     */
    private List<Row> stationPersionClassification(List<Row> rows) {
        return null;
    }
}
