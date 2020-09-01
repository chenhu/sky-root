package com.sky.signal.pre.processor.odAnalyze;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 为区县od分析生成停留点列表
 */
@Service("stayPointProcessor")
public class StayPointProcessor implements Serializable {
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SignalLoader signalLoader;
    @Autowired
    private StayPointUtil stayPointUtil;

    private List<Row> timeAmendment(List<Row> stayPoints, List<Row> movePoints) {
        List<Row> result = new ArrayList<>();
        Row prev = null;
        int j = 0; // move points的指针
        for (int i = 0; i < stayPoints.size(); i++) {
            Row current = stayPoints.get(i);
            if (prev == null) {
                prev = current;
            } else {
                // 在处理OD对之前先把前面的位移点加入结果
                while (j < movePoints.size() && stayPointUtil.getTimeDiff((Timestamp) movePoints
                        .get(j).getAs("last_time"), (Timestamp) prev.getAs("begin_time")) >= 0) {
                    result.add(movePoints.get(j));
                    j++;
                }
                Timestamp prevEnd = prev.getAs("last_time");
                Timestamp currentBegin = current.getAs("begin_time");
                //处理已经合并的停留点：删除合并前中间的位移点
                while (j < movePoints.size() && stayPointUtil.getTimeDiff((Timestamp) movePoints
                        .get(j).getAs("begin_time"), (Timestamp) prev.getAs("last_time")) > 0) {
                    j++;
                }
                // 合并点
                int moveStart = j;
                int newPrev = -1;
                int newCur = -1;
                Row o_max = null;
                Row d_min = null;
                Row first = null; //newPrev的后一个row
                while (j < movePoints.size() && stayPointUtil.getTimeDiff((Timestamp) movePoints
                        .get(j).getAs("last_time"), (Timestamp) current.getAs("begin_time")) >= 0) {
                    Row moveCurrent = movePoints.get(j);
                    if (prev.getAs("lng").equals(moveCurrent.getAs("lng")) && prev.getAs("lat")
                            .equals(moveCurrent.getAs("lat"))) {
                        o_max = moveCurrent;
                        newPrev = j;
                        if (j + 1 < movePoints.size() && stayPointUtil.getTimeDiff((Timestamp)
                                movePoints.get(j + 1).getAs("last_time"), (Timestamp) current
                                .getAs("begin_time")) >= 0) {
                            first = movePoints.get(j + 1);
                        }
                    }
                    if (d_min == null && current.getAs("lng").equals(moveCurrent.getAs("lng")) &&
                            current.getAs("lat").equals(moveCurrent.getAs("lat"))) {
                        d_min = moveCurrent;
                        newCur = j;
                    }
                    j++;
                }
                if (d_min != null) {
                    currentBegin = d_min.getAs("begin_time");
                }
                if (o_max != null) {
                    prevEnd = o_max.getAs("last_time");
                }

                // OD 之间无位移点的情况
                // 1. OD 之间本来就无位移点
                // 2. 通过合并，删除了本来有的所有位移点
                if (j - moveStart == 0 || newCur == moveStart ||
                        (d_min == null && newPrev == j - 1) ||
                        stayPointUtil.getTimeDiff(prevEnd, currentBegin) <= 0) {
                    int distance = MapUtil.getDistance((double) current.getAs("lng"), (double)
                            current.getAs("lat"), (double) prev.getAs("lng"), (double) prev.getAs
                            ("lat"));
                    int move_time = stayPointUtil.getTimeDiff((Timestamp) prev.getAs
                            ("begin_time"), currentBegin);
                    result.add(new GenericRowWithSchema(new Object[]{prev.getAs("date"), prev
                            .getAs("msisdn"), prev.getAs("base"), prev.getAs("lng"), prev.getAs
                            ("lat"), prev.getAs("begin_time"), prevEnd,prev.getAs("city_code"), prev.getAs("district_code"), distance, move_time,
                            MapUtil.formatDecimal(move_time == 0 ? 0 : (double) distance /
                                    move_time * 3.6, 2), prev.getAs("point_type")},
                            ODSchemaProvider.TRACE_SCHEMA));

                } else {
                    // 重新计算时间
                    first = first == null ? movePoints.get(moveStart) : first;
                    Timestamp moveBegin = first.getAs("begin_time");
                    int distance = MapUtil.getDistance((double) prev.getAs("lng"), (double) prev
                            .getAs("lat"), (double) first.getAs("lng"), (double) first.getAs
                            ("lat"));
                    if (stayPointUtil.getTimeDiff(prevEnd, moveBegin) >= 600) {
                        int delta = Math.round(distance * 3.6f / 8);
                        Timestamp newEnd = new Timestamp(moveBegin.getTime() - delta * 1000);
                        prevEnd = stayPointUtil.getTimeDiff(prevEnd, newEnd) > 0 ? newEnd : prevEnd;
                    }

                    int move_time = stayPointUtil.getTimeDiff((Timestamp) prev.getAs
                            ("begin_time"), moveBegin);
                    result.add(new GenericRowWithSchema(new Object[]{prev.getAs("date"), prev
                            .getAs("msisdn"), prev.getAs("base"), prev.getAs("lng"), prev.getAs
                            ("lat"), prev.getAs("begin_time"), prevEnd,prev.getAs("city_code"), prev.getAs("district_code"), distance, move_time,
                            MapUtil.formatDecimal(move_time == 0 ? 0 : (double) distance /
                                    move_time * 3.6, 2), prev.getAs("point_type")},
                            ODSchemaProvider.TRACE_SCHEMA));

                    if (newPrev == -1 && newCur == -1) {
                        result = addToList(moveStart, j, movePoints, result);
                    } else if (newPrev == -1) {
                        result = addToList(moveStart, newCur, movePoints, result);
                    } else if (newCur == -1) {
                        result = addToList(newPrev + 1, j, movePoints, result);
                    } else {
                        result = addToList(newPrev + 1, newCur, movePoints, result);
                    }
                }
                prev = new GenericRowWithSchema(new Object[]{current.getAs("date"), current.getAs
                        ("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs
                        ("lat"), currentBegin, current.getAs("last_time"),current.getAs("city_code"), current.getAs("district_code"), current.getAs
                        ("distance"), current.getAs("move_time"), current.getAs("speed"), current
                        .getAs("point_type")}, ODSchemaProvider.TRACE_SCHEMA);
            }
        }
        // 最后的处理
        if (prev != null) {
            result.add(prev);
            while (j < movePoints.size() && stayPointUtil.getTimeDiff((Timestamp) movePoints.get
                    (j).getAs("begin_time"), (Timestamp) prev.getAs("last_time")) > 0) {
                j++;
            }
        }
        while (j < movePoints.size()) {
            result.add(movePoints.get(j));
            j++;
        }

        return result;
    }

    private List<Row> addToList(int start, int end, List<Row> list, List<Row> result) {
        for (int i = start; i < end; i++) {
            result.add(list.get(i));
        }
        return result;
    }

    /**
     * 因为是按照全省范围处理od分析，会出现地市切换的情况，如果出现切换地市，则把pre的moveTime设置为0
     * 这个步骤要在停留点判定之前做，就可以把地市交界的停留点给去掉
     * @param rows
     * @return
     */
    private List<Row> changeMoveTime(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        //排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return (Timestamp) row.getAs("begin_time");
            }
        });
        //按出发时间排序
        rows = ordering.sortedCopy(rows);
        Row pre = null;
        Row current;
        int loops = 0;
        for (Row row : rows) {
            current = row;
            loops++;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                if(!pre.getAs("city_code").equals(current.getAs("city_code"))) {
                    //设置pre的到达时间和离开时间一样
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("region"),
                            pre.getAs("city_code"),
                            pre.getAs("district_code"),
                            pre.getAs("tac"),
                            pre.getAs("cell"),
                            pre.getAs("base"),
                            pre.getAs("lng"),
                            pre.getAs("lat"),
                            pre.getAs("begin_time"),
                            pre.getAs("begin_time"),//设置跟begintime一致，为了让这个点肯定会被忽略掉
                            pre.getAs("distance"),
                            0,
                            pre.getAs("speed")
                    }, SignalSchemaProvider.SIGNAL_SCHEMA_BASE_1);
                    result.add(mergedRow);
                } else {
                    result.add(pre);
                }
                pre = current;
                if(loops == rows.size()) {
                    result.add(current);
                }
            }
        }
        return result;
    }

    /**
     * 把停留点加入到列表
     * @param rows
     * @return
     */
    private List<Row> createPointList(List<Row> rows) {
        //List保存所有O点和D点，用于区县OD分析
        List<Row> pointList = new ArrayList<>();
        if (rows.size() < 2) {
            return pointList;
        }
        for(Row row: rows) {
            if ((Byte) row.getAs("point_type") == SignalProcessUtil.STAY_POINT) {
                pointList.add(row);
            }
        }

        return pointList;
    }
    public void process(String validSignalFile) {
        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        DataFrame df;
        df = signalLoader.load(validSignalFile);
        //手机号码->信令数据
        JavaPairRDD<String, Row> rdd1 = df.javaRDD().mapToPair(new PairFunction<Row, String, Row>
                () {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String msisdn = row.getAs("msisdn");
                Integer date = row.getAs("date");
                return new Tuple2<>(msisdn + "|" + date, row);
            }
        });

        //将同一手机号码的信令数据聚合到一个List中, 重新分区
        List<Row> rows = Lists.newArrayList();
        JavaPairRDD<String, List<Row>> rdd2 = rdd1.aggregateByKey(rows, params.getPartitions(),
                new Function2<List<Row>, Row, List<Row>>() {
                    @Override
                    public List<Row> call(List<Row> rows, Row row) throws Exception {
                        rows.add(row);
                        return rows;
                    }
                }, new Function2<List<Row>, List<Row>, List<Row>>() {
                    @Override
                    public List<Row> call(List<Row> rows1, List<Row> rows2) throws Exception {
                        rows1.addAll(rows2);
                        return rows1;
                    }
                });

        //按手机号码进行停驻点分析
        JavaRDD<Row> pointRdd = rdd2.values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com
                        .google.common.base.Function<Row, Timestamp>() {
                    @Override
                    public Timestamp apply(Row row) {
                        return row.getAs("begin_time");
                    }
                });
                //处理按照全省级别进行od分析的时候，出现在地市边界的问题
                rows = changeMoveTime(rows);
                rows = stayPointUtil.determinePointType(rows);

                //按startTime排序
                rows = ordering.sortedCopy(rows);
                // 连续确定停留点和可能停留点合并
                int beforeSize, afterSize;
                do {
                    beforeSize = rows.size();
                    rows = stayPointUtil.mergeContinuePoint(rows);
                    afterSize = rows.size();
                } while (beforeSize - afterSize > 0);

                // 单独抽出确定停留点和可能停留点，合并连续确定停留点
                Tuple2<List<Row>, List<Row>> pointSplit = stayPointUtil.mergeStayPoint(rows, new ArrayList<Row>());
                rows = pointSplit._1();
                List<Row> moveList = pointSplit._2();
                // 判断可能停留点状态
                rows = stayPointUtil.mergeUncertainPoint(rows);
                // 循环至所有确定停留点间距 > RANGE_I
                do {
                    beforeSize = rows.size();
                    pointSplit = stayPointUtil.mergeStayPoint(rows, moveList);
                    rows = pointSplit._1();
                    moveList = pointSplit._2();
                    afterSize = rows.size();
                } while (beforeSize - afterSize > 0);
                moveList = ordering.sortedCopy(moveList);
                rows = ordering.sortedCopy(rows);
                rows = timeAmendment(rows, moveList);
                return createPointList(rows);
            }
        });
        String date = validSignalFile.substring(validSignalFile.length() - 8);
        if (params.getRunMode().equals("district")) {
            DataFrame pointDF = sqlContext.createDataFrame(pointRdd, ODSchemaProvider.TRACE_SCHEMA);

            FileUtil.saveFile(pointDF.repartition(partitions), FileUtil.FileType.PARQUET, params.getPointPath(params.getDistrictCode().toString(), date));
        } else if(params.getRunMode().equals("province")) {
            DataFrame pointDF = sqlContext.createDataFrame(pointRdd, ODSchemaProvider.TRACE_SCHEMA);
            FileUtil.saveFile(pointDF.repartition(partitions), FileUtil.FileType.PARQUET, params.getPointPath(date));
        }
    }
}

