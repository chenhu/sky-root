package com.sky.signal.pre.processor.transportationhub;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import scala.Tuple2;
import scala.Tuple3;

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
    @Autowired
    private transient SignalLoader signalLoader;
    /*************************************/

    /**
     * 每次只处理一天的数据
     * 1. 过滤有效信令，把经过枢纽基站的用户当天轨迹保留下来
     * 2. 合并枢纽基站
     * 3. 给每条信令增加停留点类型
     *
     * @param validSignalFile 一天的有效信令路径
     * @return 当天经过枢纽基站的用户全部轨迹
     */
    public DataFrame filterData(String validSignalFile) {

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        DataFrame validSignalDf = signalLoader.load(validSignalFile);
        //遍历有效信令，找到每个用户哪些天经过枢纽基站，并替换有效信令中位置信息为虚拟基站
        //信令数据转换为手机号码和记录的键值对
        JavaPairRDD<String, List<Row>> validSignalPairRDD = SignalProcessUtil
                .signalToJavaPairRDD(validSignalDf, params);
        // 加载枢纽基站
        final Broadcast<Map<String, Row>> stationCell = cellLoader.load();
        // 过滤信令数据，如果用户信令中出现枢纽基站，则保留这个用户的数据
        validSignalPairRDD = validSignalPairRDD.filter(new Function<Tuple2<String, List<Row>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, List<Row>> msisdnSignal)
                    throws Exception {
                for (Row row : msisdnSignal._2) {
                    String base = (String) row.getAs("base");
                    if (!StringUtils.isEmpty(base)) {
                        if (stationCell.value().containsKey(base)) {
                            return true;
                        }
                    }
                }
                return false;
            }
        });

        JavaRDD<List<Row>> stationBaseRDD = validSignalPairRDD.values().map
                (new Function<List<Row>, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst()
                        .onResultOf(new com.google.common.base.Function<Row,
                                Timestamp>() {
                    @Override
                    public Timestamp apply(Row row) {
                        return (Timestamp) row.getAs("begin_time");
                    }
                });

                //按startTime排序
                rows = ordering.sortedCopy(rows);
                //合并枢纽基站
                rows = mergeStationBase(rows);
                //重新排序
                rows = ordering.sortedCopy(rows);
                //判定停留点类型
                rows = stayPointDecision(rows);
                return rows;
            }
        });
        JavaRDD<Row> resultRDD = stationBaseRDD.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return rows;
            }
        });
        DataFrame resultDf = sqlContext.createDataFrame(resultRDD,
                ODSchemaProvider.TRACE_SCHEMA);
        String date = validSignalFile.substring(validSignalFile.length() - 8);
        FileUtil.saveFile(resultDf.repartition(partitions), FileUtil.FileType
                .CSV, params.getSavePath() + PathConfig.STATION_DATA_PATH +
                date);
        return resultDf;

    }

    /**
     * 增加并判定停留点类型
     * 针对每一条手机数据，计算当前位置点与后一位置点的时间差t及空间点位移速度v; 若该数据所占用的基站是枢纽站，t >= 10min
     * 则判断当前位置为停留点，t < 10min 则为位移点；若当前数据为非枢纽站数据，t >= 40min则当前位置点为确定停留点,若10min
     * <= t < 40min 且 v < 8km/h 则为可能停留点，否则为位移点。
     *
     * @param rows 每个用户一天的信令，并已经按照开始时间升序排列
     * @return 增加了停留点的用户信令数据
     */
    private List<Row> stayPointDecision(List<Row> rows) {
        // 带停留点类型的结果列表数据
        List<Row> resultSignalList = new ArrayList<>(rows.size());
        // 前一条信令
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            // 当前信令
            Row current = rows.get(i);
            // 增加了停留点类型的行
            Row stayPointRow = null;
            if (prior == null) {
                prior = current;
            } else {
                Timestamp beginTime = (Timestamp) prior.getAs("begin_time");
                Timestamp endTime = (Timestamp) current.getAs("last_time");
                stayPointRow = SignalProcessUtil.getNewRowWithStayPoint(prior, current, beginTime,
                        endTime);
            }
            //最后一条信令,或者只有一条信令
            //1. 如果信令在枢纽基站，则修改后，加入结果列表，并跳出循环
            //2. 如果信令不在枢纽基站，加入结果列表，并跳出循环
            if (i == rows.size() - 1) {
                Timestamp beginTime = (Timestamp) prior.getAs("begin_time");
                Timestamp endTime = (Timestamp) prior.getAs("last_time");
                stayPointRow = SignalProcessUtil.getNewRowWithStayPoint(prior, null, beginTime, endTime);
                resultSignalList.add(stayPointRow);
                break;
            }
            // 为下次循环重新设置前一个记录指向当前记录
            prior = current;
            // 增加了停留点字段的记录增加到结果集
            resultSignalList.add(stayPointRow);
        }
        return resultSignalList;
    }

    /**
     * 合并相邻枢纽基站
     * 对同一MSISDN按照字段（start_time）排序，对连续两条数据{A-B}，若A和B都是枢纽基站，
     * 则合并A和B为新的虚拟基站X，记录A的start_time为X的start_time，
     * B的last_time为X的last_time，并计算新基站X
     * 与下一条轨迹数据所在基站的distance、move_time、speed
     *
     * @param rows 用户一天的手机信令数据
     * @return 合并后的手机信令数据
     */
    private List<Row> mergeStationBase(List<Row> rows) {
        //合并连续枢纽基站后的单用户一天的轨迹，并且是按照时间升序排序
        List<Row> mergedBaseSignalList = new ArrayList<>(rows.size());
        // 加载枢纽基站
        final Broadcast<Map<String, Row>> stationCell = cellLoader.load();
        // 前一条信令
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            // 当前信令
            Row current = rows.get(i);
            // 虚拟基站
            Row visualBaseRow = null;
            if (prior == null) {
                prior = current;
            } else {
                String priorBase = (String) prior.getAs("base");
                String currentBase = (String) current.getAs("base");
                Timestamp beginTime = (Timestamp) prior.getAs("begin_time");
                Timestamp lastTime = (Timestamp) current.getAs("last_time");

                if (stationCell.value().containsKey(priorBase)) {
                    // 连续的枢纽基站，合并为虚拟基站,并重新计算与下个点的距离、移动时间、速度
                    if (stationCell.value().containsKey(currentBase)) {
                        //合并,默认为最后一条记录，距离和速度均为0
                        visualBaseRow = new GenericRowWithSchema(new
                                Object[]{prior.getAs("date"), prior.getAs
                                ("msisdn"), params.getVisualStationBase(),
                                params.getVisualLng(), params.getVisualLat(),
                                beginTime, lastTime, 0d, prior.getAs
                                ("move_time"), 0d}, ODSchemaProvider
                                .STATION_TRACE_SCHEMA);
                        if (i < rows.size() - 1) {// 不是最后一条记录,
                            // 取出下条记录，并重新计算与下个点的距离、移动时间、速度
                            Row next = rows.get(i + 1);
                            Tuple3<Integer, Integer, Double> tuple3 =
                                    SignalProcessUtil
                                            .getDistanceMovetimeSpeed
                                                    (visualBaseRow, next,
                                                            beginTime,
                                                            lastTime);
                            //重新计算距离、逗留时间、速度
                            visualBaseRow = new GenericRowWithSchema(new
                                    Object[]{prior.getAs("date"), prior.getAs
                                    ("msisdn"), params.getVisualStationBase()
                                    , params.getVisualLng(), params
                                    .getVisualLat(), beginTime, lastTime,
                                    tuple3._1(), tuple3._2(), tuple3._3()},
                                    ODSchemaProvider.STATION_TRACE_SCHEMA);
                        }
                    } else { //非连续的枢纽基站，把枢纽基站替换为虚拟基站,并重新计算与下个点的的距离、移动时间、速度
                        visualBaseRow = new GenericRowWithSchema(new
                                Object[]{prior.getAs("date"), prior.getAs
                                ("msisdn"), params.getVisualStationBase(),
                                params.getVisualLng(), params.getVisualLat(),
                                prior.getAs("begin_time"), prior.getAs
                                ("last_time"), prior.getAs("distance"), prior
                                .getAs("move_time"), prior.getAs("speed")},
                                ODSchemaProvider.STATION_TRACE_SCHEMA);
                    }
                    // 设置前条信令指向新计算的信令
                    prior = visualBaseRow;
                }
            }

            //最后一条信令,或者只有一条信令
            //1. 如果信令在枢纽基站，则修改后，加入结果列表，并跳出循环
            //2. 如果信令不在枢纽基站，加入结果列表，并跳出循环
            if (i == rows.size() - 1) {
                String base = (String) current.getAs("base");
                if (stationCell.value().containsKey(base)) {
                    //默认为最后一条记录，距离和速度均为0
                    Row lastRow = new GenericRowWithSchema(new
                            Object[]{current.getAs("date"), current.getAs
                            ("msisdn"), params.getVisualStationBase(), params
                            .getVisualLng(), params.getVisualLat(), current
                            .getAs("last_time"), current.getAs("distance"),
                            current.getAs("move_time"), current.getAs
                            ("speed")}, ODSchemaProvider.STATION_TRACE_SCHEMA);
                    mergedBaseSignalList.add(lastRow);
                } else {
                    mergedBaseSignalList.add(current);
                }
                //跳出循环，防止重复增加
                break;
            }
            //把前条信令加入到结果列表中
            //1. prior的基站不是枢纽基站，不做更改，直接加入结果列表
            //2. prior的基站是枢纽基站，但是当前基站不是枢纽基站，即：非连续枢纽基站，把更新基站后的prio加入结果列表
            //3. prior和current都是枢纽基站，则合并成一条虚拟基站，并重新计算距离、逗留时间、速度后，加入结果列表
            //4. current是最后一条记录，判定是否是枢纽基站，并加入到结果列表
            mergedBaseSignalList.add(visualBaseRow);
        }
        return mergedBaseSignalList;
    }
}
