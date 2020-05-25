package com.sky.signal.pre.processor.transportationhub;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.SignaProcesslUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Chenhu on 2020/5/25.
 * 从有效信令中，取出经过枢纽站的用户轨迹
 * 如果当天用户经过枢纽站，则保留用户全天轨迹
 */
@Component
public class StationDataFilter implements Serializable {

    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    // 基站加载
    @Autowired
    private transient CellLoader cellLoader;


    /**
     * 过滤有效信令，把经过枢纽基站的用户当天轨迹保留下来
     * 每次只处理一天的数据
     *
     * @param validSignalDf 一天的有效信令
     * @return 当天经过枢纽基站的用户全部轨迹
     */
    public DataFrame filterData(DataFrame validSignalDf) {

        //遍历有效信令，找到每个用户哪些天经过枢纽基站，并替换有效信令中位置信息为虚拟基站
        //信令数据转换为手机号码和记录的键值对
        JavaPairRDD<String, List<Row>> validSignalPairRDD = SignaProcesslUtil
                .signalToJavaPairRDD(validSignalDf, params);
        // 加载枢纽基站
        final Broadcast<Map<String, Row>> stationCell = cellLoader.load
                (params.getBasePath() + PathConfig.STATION_CELL_PATH);
        // 取到枢纽基站的虚拟基站
        final String position = String.format("%.6f|%.6f", params
                .getVisualLng(), params.getVisualLat());
        // 过滤信令数据，如果用户信令中出现枢纽基站，则保留这个用户的数据
        validSignalPairRDD = validSignalPairRDD.filter(new Function<Tuple2<String, List<Row>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, List<Row>> msisdnSignal)
                    throws Exception {
                for (Row row : msisdnSignal._2) {
                    String base = row.getAs("base");
                    if (!StringUtils.isEmpty(base)) {
                        if (stationCell.value().containsKey(base)) {
                            return true;
                        }
                    }
                }
                return false;
            }
        });

        JavaRDD<List<Row>> stationBaseRDD = validSignalPairRDD.values().map(new Function<List<Row>, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst()
                        .onResultOf(new com.google.common.base.Function<Row,
                                Timestamp>() {
                            @Override
                            public Timestamp apply(Row row) {
                                return row.getAs("begin_time");
                            }
                        });

                //按startTime排序
                rows = ordering.sortedCopy(rows);
                rows = mergeStationBase(rows);

                return rows;
            }
        });

//        // 对出现在枢纽基站的用户信令的枢纽基站base替换为虚拟基站
//        validSignalPairRDD = validSignalPairRDD.mapValues(new Function<List<Row>, List<Row>>() {
//            @Override
//            public List<Row> call(List<Row> rows) throws Exception {
//                List<Row> stationBaseSignal = new ArrayList<>(rows.size());
//                for (Row row : rows) {
//                    String base = row.getAs("base");
//                    if (!StringUtils.isEmpty(base)) {
//                        if (stationCell.value().containsKey(base)) {
//                            Row stationBaseRow = new GenericRowWithSchema(new
//                                    Object[]{row.getAs("date"), row.getAs
//                                    ("msisdn"), row.getAs("region"), row
//                                    .getAs("city_code"), row.getAs
//                                    ("cen_region"), row.getAs("sex"), row
//                                    .getAs("age"), row.getAs("tac"), row
//                                    .getAs("cell"), position, params
//                                    .getVisualLng(), params.getVisualLat(),
//                                    row.getAs("begin_time"), row.getAs
//                                    ("last_time"), row.getAs("distance"), row
//                                    .getAs("move_time"), row.getAs("speed")},
//                                    SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA);
//                            stationBaseSignal.add(stationBaseRow);
//                        } else {
//                            stationBaseSignal.add(row);
//                        }
//                    } else {
//                        stationBaseSignal.add(row);
//                    }
//                }
//                return stationBaseSignal;
//            }
//        });

//        JavaRDD<Row> stationBaseRDD = validSignalPairRDD.values().flatMap(new FlatMapFunction<List<Row>, Row>() {
//            @Override
//            public Iterable<Row> call(List<Row> rows) throws Exception {
//                return rows;
//            }
//        });
        JavaRDD<Row> resultRDD = stationBaseRDD.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return rows;
            }
        });
        return sqlContext.createDataFrame(resultRDD,
                SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA);
    }

    /**
     * 合并连续的枢纽基站为虚拟枢纽基站
     * @param rows 用户一天的手机信令数据
     * @return 合并后的手机信令数据
     */
    private List<Row> mergeStationBase(List<Row> rows) {
        //合并连续枢纽基站后的单用户一天的轨迹，并且是按照时间升序排序
        List<Row> mergedBaseSignalList = new ArrayList<>(rows.size());
        // 加载枢纽基站
        final Broadcast<Map<String, Row>> stationCell = cellLoader.load
                (params.getBasePath() + PathConfig.STATION_CELL_PATH);
        // 取到枢纽基站的虚拟基站
        final String position = String.format("%.6f|%.6f", params
                .getVisualLng(), params.getVisualLat());
        // 前一条信令
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            // 当前信令
            Row current = rows.get(i);
            if (prior == null) {
                prior = current;
            } else {
                String priorBase = prior.getAs("base");
                String currentBase = current.getAs("base");
                if (stationCell.value().containsKey(priorBase)) {
                    // 连续的枢纽基站，合并为虚拟基站,并重新计算与下个点的距离、移动时间、速度
                    if (stationCell.value().containsKey(currentBase)) {

                    } else { //非连续的枢纽基站，把枢纽基站替换为虚拟基站,并重新计算与下个点的的距离、移动时间、速度

                    }
                }


            }

        }
        return mergedBaseSignalList;
    }
}