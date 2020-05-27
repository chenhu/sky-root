package com.sky.signal.pre.processor.transportationhub;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/25.
 * 枢纽站人口分类器
 */
@Component
public class StationPersonClassification implements Serializable {
    @Autowired
    private transient ParamProperties params;
    public void process(String validSignalFile) {

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
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
