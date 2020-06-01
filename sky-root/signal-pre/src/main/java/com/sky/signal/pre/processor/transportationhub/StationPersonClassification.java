package com.sky.signal.pre.processor.transportationhub;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.processor.transportationhub.StationPersonClassify
        .KunShanStation;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.MapUtil;
import com.sky.signal.pre.util.ProfileUtil;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/25.
 * 枢纽站人口分类器
 */
@Component
public class StationPersonClassification implements Serializable {
    //60分钟
    private static final Integer ONE_HOUR = 60 * 60;
    private static final Integer TEN_MI = 10 * 60;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient KunShanStation kunShanStation;
    @Autowired
    private transient StationPersonClassifyUtil stationPersonClassifyUtil;

    public void process(String stationTraceFilePath) {

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        final String date = stationTraceFilePath.substring
                (stationTraceFilePath.length() - 8);

        DataFrame stationTraceDf = FileUtil.readFile(FileUtil.FileType.CSV,
                ODSchemaProvider.TRACE_SCHEMA, stationTraceFilePath)
                .repartition(params.getPartitions());
        // 数据转化为以手机号为key，整行数据为value的KeyPair数据
        JavaPairRDD<String, List<Row>> stationTracePairRDD =
                SignalProcessUtil.signalToJavaPairRDD(stationTraceDf, params);
        // 加载所有枢纽站轨迹有效信令数据，因为昆山南站的分析需要用到昨天和明天的信令
        //需要在map外部预先取出这个数据集
        final DataFrame stationTrace = loadStationTrace();
        JavaRDD<List<Row>> stationTraceRDD = stationTracePairRDD.values().map
                (new Function<List<Row>, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows) throws Exception {
                return personClassification(rows, stationTrace);
            }
        });

        JavaRDD<Row> resultRDD = stationTraceRDD.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return rows;
            }
        });
        DataFrame resultDf = sqlContext.createDataFrame(resultRDD,
                ODSchemaProvider.STATION_TRACE_CLASSIC_SCHEMA);

        FileUtil.saveFile(resultDf.repartition(partitions), FileUtil.FileType
                .CSV, params.getSavePath() +
                PathConfig.STATION_CLASSIC_PATH + date);
    }

    /**
     * 人口分类
     * 针对每个用户（以MSISDN字段为区分），对move_time进行求和，计算其当天全部的逗留时间；对于逗留时间 >=
     * 60min的用户，直接进入第⑤步，对于逗留时间 < 60min的用户，直接进入步骤④进行铁路过境人口判断
     *
     * @param rows 用户一天的信令数据
     * @return 人口分类
     */
    private List<Row> personClassification(List<Row> rows, DataFrame
            stationTrace) {
        List<Row> resultList;
        Long sumMoveTime = 0L;
        for (Row row : rows) {
            Integer moveTime = row.getAs("move_time");
            sumMoveTime += moveTime;
        }
        if (sumMoveTime >= ONE_HOUR) {
            resultList = stationPersionClassification(rows);
        } else {
            resultList = kunShanStation.classify(rows, stationTrace);
        }
        return resultList;
    }

    /**
     * 枢纽站人口分类
     *
     * @param rows 用户一条的信令
     * @return 增加分类后的用户数据
     */
    private List<Row> stationPersionClassification(List<Row> rows) {
        int stationCount = getStationCount(rows);
        if (stationCount < 2) { //枢纽基站为1个
            rows = stationPersonClassifyUtil.oneStationBaseProc(rows);
        } else { //合并基站
            rows = mergeStationBase(rows);
            //重新计算枢纽基站数量
            stationCount = getStationCount(rows);
            if (stationCount < 2) {
                rows = stationPersonClassifyUtil.oneStationBaseProc(rows);
            } else if (stationCount == 2) {
                rows = stationPersonClassifyUtil.twoStationBaseProc(rows);
            } else {
                rows = stationPersonClassifyUtil.gt2StationBaseProc(rows);
            }
        }

        return rows;
    }

    /**
     * 对于枢纽基站数量大于1的情况，进行基站合并
     * <p>
     * <pre>
     *     计算相邻两个虚拟基站{Ai,Ai+1}的时间间隔T = Ai+1(start_time) – Ai(last_time)：
     * 若 T <= 10min，合并两个枢纽站Ai,
     * Ai+1，删除中间的其他点，以Ai的（start_time）为新枢纽点Ai’的（start_time），Ai+1的（last
     * _time）为新枢纽点Ai’的（last
     * _time），重新计算distance、move_time、speed，并记Ai’为该用户当天轨迹中有效的枢纽点；
     * 若 10min < T < 2hr，舍弃枢纽点Ai；
     * 若 T >= 2hr，将Ai,Ai+1分别记为该用户当天轨迹中有效的两个枢纽点。
     * 循环处理集合Ai中的所有虚拟基站，得到记录该用户当天新的轨迹集合Qi = {Q1,Q2,…,Qn}，以及轨迹中所有有效枢纽点的新集合Si =
     * {S1,S2,…,Sn}
     *
     * </pre>
     *
     * @param rows 用户一天的信令数据
     * @return 合并虚拟基站后的信令
     */
    private List<Row> mergeStationBase(List<Row> rows) {
        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());

        //需要按照时间排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("begin_time");
            }
        });
        //按begin_time排序
        rows = ordering.sortedCopy(rows);
        Row priorStationBase = null, currentStationBase = null;
        int priorStationBaseIndex = -1, currentStationBaseIndex = -1;


        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).getAs("base").equals(params.getVisualStationBase
                    ())) {
                currentStationBase = rows.get(i);
                currentStationBaseIndex = i;
            }
            if (priorStationBase == null && currentStationBase != null) {
                priorStationBase = currentStationBase;
                priorStationBaseIndex = i;
            } else if (priorStationBase != null && currentStationBase != null) {
                DateTime beginTime = new DateTime(priorStationBase.getAs
                        ("last_time"));
                DateTime endTime = new DateTime(currentStationBase.getAs
                        ("begin_time"));
                if (Math.abs(Seconds.secondsBetween(beginTime, endTime)
                        .getSeconds()) <= TEN_MI) { //小于等于10分钟，合并两个虚拟基站

                    Row mergedStationBaseRow = SignalProcessUtil.getNewRowWithStayPoint
                            (currentStationBase, rows.get(i+1), (Timestamp) priorStationBase
                                    .getAs("begin_time"),(Timestamp) currentStationBase
                                    .getAs("last_time"));
                    //合并虚拟基站后的记录添加到结果列表中
                    resultList.add(mergedStationBaseRow);
                }

            } else { //非枢纽基站
                resultList.add(rows.get(i));
            }

        }


        return null;
    }

    /**
     * 获取枢纽轨迹数据中枢纽基站的数量
     *
     * @param rows 用户一天的枢纽轨迹
     * @return 用户一天枢纽轨迹中经过枢纽基站的次数
     */
    private int getStationCount(List<Row> rows) {
        int stationCount = 0;
        for (Row row : rows) {
            if (row.getAs("base").equals(params.getVisualStationBase())) {
                stationCount++;
            }
        }
        return stationCount;
    }

    /**
     * description: 加载带停留点类型的枢纽站轨迹有效信令数据
     * return: org.apache.spark.sql.DataFrame
     **/
    private DataFrame loadStationTrace() {
        //获取所有的枢纽站轨迹信令
        return FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider
                .TRACE_SCHEMA, params.getSavePath().concat(PathConfig
                .STATION_DATA_PATH).concat("*")).select("date", "msisdn",
                "point_type").repartition(params.getPartitions()).persist
                (StorageLevel.MEMORY_AND_DISK());

    }

}
