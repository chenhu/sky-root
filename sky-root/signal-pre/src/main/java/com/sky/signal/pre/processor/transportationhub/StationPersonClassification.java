package com.sky.signal.pre.processor.transportationhub;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.processor.transportationhub
        .StationPersonClassify.KunShanStation;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/25.
 * 枢纽站人口分类器
 */
@Component
public class StationPersonClassification implements Serializable {
    //60分钟
    private static final Integer ONE_HOUR = 60 * 60;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient KunShanStation kunShanStation;

    public void process(String stationTraceFilePath) {

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        final String date = stationTraceFilePath.substring
                (stationTraceFilePath
                        .length() - 8);

        DataFrame stationTraceDf = FileUtil.readFile(FileUtil
                        .FileType.CSV,
                ODSchemaProvider.TRACE_SCHEMA, stationTraceFilePath)
                .repartition(params.getPartitions());
        // 数据转化为以手机号为key，整行数据为value的KeyPair数据
        JavaPairRDD<String, List<Row>> stationTracePairRDD =
                SignalProcessUtil.signalToJavaPairRDD
                        (stationTraceDf, params);
        // 加载所有枢纽站轨迹有效信令数据，因为昆山南站的分析需要用到昨天和明天的信令
        //需要在map外部预先取出这个数据集
        final DataFrame stationTrace = loadStationTrace();
        JavaRDD<List<Row>> stationTraceRDD = stationTracePairRDD
                .values().map
                        (new Function<List<Row>, List<Row>>() {
                            @Override
                            public List<Row> call(List<Row> rows)
                                    throws
                                    Exception {
                                return personClassification(rows,
                                        stationTrace);
                            }
                        });

        JavaRDD<Row> resultRDD = stationTraceRDD.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws
                    Exception {
                return rows;
            }
        });
        DataFrame resultDf = sqlContext.createDataFrame(resultRDD,
                ODSchemaProvider.STATION_TRACE_CLASSIC_SCHEMA);

        FileUtil.saveFile(resultDf.repartition(partitions),
                FileUtil.FileType.CSV, params.getSavePath() +
                        PathConfig.STATION_CLASSIC_PATH + date);
    }

    /**
     * 人口分类
     * 针对每个用户（以MSISDN字段为区分），对move_time进行求和，计算其当天全部的逗留时间；对于逗留时间 >=
     * 60min的用户，直接进入第⑤步，对于逗留时间 < 60min的用户，直接进入步骤④进行铁路过境人口判断
     *
     * @param rows 用户一天的信令数据
     * @return
     */
    private List<Row> personClassification(List<Row> rows,
                                           DataFrame stationTrace) {
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

        return null;
    }

    /**
     * description: 加载带停留点类型的枢纽站轨迹有效信令数据
     * return: org.apache.spark.sql.DataFrame
     **/
    private DataFrame loadStationTrace() {
        //获取所有的枢纽站轨迹信令
        return FileUtil.readFile(FileUtil.FileType.CSV,
                ODSchemaProvider.TRACE_SCHEMA,
                params.getSavePath().concat(
                        PathConfig.STATION_DATA_PATH).concat("*"))
                .select("date", "msisdn", "point_type")
                .repartition(params.getPartitions()).persist
                        (StorageLevel.MEMORY_AND_DISK());

    }

}
