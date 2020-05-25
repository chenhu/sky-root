package com.sky.signal.pre.processor.transportationhub;

import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Created by Chenhu on 2020/5/25.
 * 枢纽站人口分类器
 */
@Component
public class StationPersonClassification implements Serializable {

    public void process(DataFrame staionValidSignal) {

    }


    /**
     * 合并相邻枢纽基站
     * 对同一MSISDN按照字段（start_time）排序，对连续两条数据{A-B}，若A和B都是枢纽基站，
     * 则合并A和B为新的虚拟基站X，记录A的start_time为X的start_time，
     * B的last_time为X的last_time，并计算新基站X
     * 与下一条轨迹数据所在基站的distance、move_time、speed
     *
     * @param df
     * @return
     */
    private DataFrame mergeStation(DataFrame df) {
        return null;
    }

    /**
     * 确定停留点类型
     * 针对每一条手机数据，计算当前位置点与后一位置点的时间差t及空间点位移速度v; 若该数据所占用的基站是枢纽站，t >= 10min
     * 则判断当前位置为停留点，t < 10min 则为位移点；若当前数据为非枢纽站数据，t >= 40min则当前位置点为确定停留点,若10min
     * <= t < 40min 且 v < 8km/h 则为可能停留点，否则为位移点。（0=确定停留点，1=可能停留点，2=位移点）
     *
     * @param df
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
     * @param df
     * @return
     */
    private DataFrame personClassification(DataFrame df) {
        return null;
    }
}
