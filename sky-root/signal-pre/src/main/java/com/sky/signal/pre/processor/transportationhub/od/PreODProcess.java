package com.sky.signal.pre.processor.transportationhub.od;

import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Chenhu on 2020/6/3.
 * 进行OD分析之前，对信令预处理
 */
@Component
public class PreODProcess implements Serializable {



    /**
     * 预处理，合并虚拟枢纽基站
     * <pre>
     *     2)	计算相邻两个虚拟基站{Ai,Ai+1}的时间间隔T = Ai+1(start_time) – Ai(last_time)：
     * 若 T <= 10min，合并两个枢纽站Ai,
     * Ai+1，删除中间的其他点，以Ai的（start_time）为新枢纽点Ai’的（start_time），Ai+1的（last
     * _time）为新枢纽点Ai’的（last _time），重新计算distance、move_time、speed、point_type
     * ，并记Ai’为该用户当天轨迹中有效的枢纽点；
     * 若 10min < T，则不合并；
     * 循环处理集合Ai中的所有虚拟基站，得到记录该用户当天新的轨迹集合Qi = {Q1,Q2,…,Qn}
     *
     * </pre>
     *
     * @param rows
     * @return
     */
    public List<Row> mergeStationBase(List<Row> rows) {
        return null;

    }

    /**
     * 合并停留点
     * <pre>
     * 1）	判断连续的可能停留点和停留点是否需要合并;
     * 2）	抽出所有可能停留点及停留点；
     * 3）	合并连续停留点；
     * 4）	判断可能停留点的状态；
     * 5）	重复步骤3）至没有连续停留点间的距离小于800m；
     * 6）	重新计算出发时间及到达时间。
     * </pre>
     *
     * @param rows
     * @return
     */
    public List<Row> mergeStayPoint(List<Row> rows) {
        return null;

    }



}
