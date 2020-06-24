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
