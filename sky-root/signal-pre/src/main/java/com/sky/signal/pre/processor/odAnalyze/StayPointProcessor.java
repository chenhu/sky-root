package com.sky.signal.pre.processor.odAnalyze;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.MapUtil;
import com.sky.signal.pre.util.MathUtil;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

/**
 * 生成出行轨迹、停驻点、出行链
 */
@Service("stayPointProcessor")
public class StayPointProcessor implements Serializable {
    // 位移点
    public static final byte MOVE_POINT = 0;
    // 可能停留点
    public static final byte UNCERTAIN_POINT = 1;
    // 停留点
    public static final byte STAY_POINT = 2;
    // 基站半径阀值
    public static final int RANGE_I = 800;
    public static final int RANGE_II = 800;
    public static final int RANGE_III = 800;
    // 停留时间小于10分钟为位移点
    public static final int STAY_TIME_MIN = 10 * 60;
    // 停留时间大于40分钟为停留点, 10-40分钟为可能停留点
    public static final int STAY_TIME_MAX = STAY_TIME_MIN * 4;
    // 可能停留点的速度阀值，为两倍步行速度
    public static final int MOVE_SPEED = 8;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SignalLoader signalLoader;

    /**
     * description: 根据逗留时间和速度，增加每行的停留点类型
     * param: [rows]
     * return: java.util.List<org.apache.spark.sql.Row>
     **/
    private static List<Row> determinePointType(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(new GenericRowWithSchema(new Object[]{row.getAs("date"), row.getAs("msisdn"), row.getAs("base"), row.getAs("lng"), row.getAs("lat"),
                    row.getAs("begin_time"), row.getAs("last_time"), row.getAs("distance"), row.getAs("move_time"),
                    row.getAs("speed"), getPointType((int) row.getAs("move_time"), (double) row.getAs("speed"))}, ODSchemaProvider.TRACE_SCHEMA));
        }
        return result;
    }

    /**
     * description: 根据逗留时间和速度，返回停留点类型
     * param: [moveTime, speed]
     * return: byte
     **/
    private static byte getPointType(int moveTime, double speed) {
        byte pointType = MOVE_POINT;
        if (moveTime >= STAY_TIME_MAX) {
            pointType = STAY_POINT;
        } else if (moveTime >= STAY_TIME_MIN && moveTime < STAY_TIME_MAX && speed < MOVE_SPEED) {
            pointType = UNCERTAIN_POINT;
        }
        return pointType;
    }

    /**
     * description: 重新计算两个移动点直接的距离、速度、和停留点类型
     * param: [prior, current, startTime, lastTime]
     * return: org.apache.spark.sql.Row
     **/
    private static Row calcRow(Row prior, Row current, Timestamp startTime, Timestamp lastTime) {
        int distance = 0;
        int moveTime = (int) (lastTime.getTime() - startTime.getTime()) / 1000;
        double speed = 0d;
        byte pointType = prior.getAs("point_type");
        if (prior != current && current != null) {
            //基站与下一基站距离
            distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) prior.getAs("lng"), (double) prior.getAs("lat"));
            //基站移动到下一基站时间 = 下一基站startTime - 基站startTime
            moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("begin_time")), new DateTime(prior.getAs("begin_time"))).getSeconds());
            //基站移动到下一基站速度
            speed = MapUtil.formatDecimal(moveTime == 0 ? 0 : (double) distance / moveTime * 3.6, 2);
        }
        pointType = pointType == UNCERTAIN_POINT ? getPointType(moveTime, speed) : pointType;
        return new GenericRowWithSchema(new Object[]{prior.getAs("date"), prior.getAs("msisdn"), prior.getAs("base"), prior.getAs("lng"), prior.getAs("lat"), startTime, lastTime, distance,
                moveTime, speed, pointType}, ODSchemaProvider.TRACE_SCHEMA);
    }


    /**
     * description: 合并连续的停留点和可能停留点
     * param: [rows]
     * return: java.util.List<org.apache.spark.sql.Row>
     **/
    private static List<Row> mergeContinuePoint(List<Row> rows) {
        List<Row> result = Lists.newArrayList();
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if (prior == null) {
                prior = current;
            } else {
                byte priorType = prior.getAs("point_type");
                byte currentType = current.getAs("point_type");
                int distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) prior.getAs("lng"), (double) prior.getAs("lat"));
                if ((priorType == STAY_POINT && currentType == STAY_POINT && distance <= RANGE_I) ||
                        (((priorType == STAY_POINT && currentType == UNCERTAIN_POINT) || (priorType == UNCERTAIN_POINT && currentType == STAY_POINT)) && distance <= RANGE_II) ||
                        (priorType == UNCERTAIN_POINT && currentType == UNCERTAIN_POINT && distance <= RANGE_III)) {
                    int moveTime1 = prior.getAs("move_time");
                    int moveTime2 = current.getAs("move_time");
                    //两点合并
                    if (moveTime1 >= moveTime2) {
                        if (i + 1 < rows.size()) {
                            prior = calcRow(prior, rows.get(i + 1), (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        } else {
                            prior = calcRow(prior, null, (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        }
                    } else {
                        if (i + 1 < rows.size()) {
                            prior = calcRow(current, rows.get(i + 1), (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        } else {
                            prior = calcRow(current, null, (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        }
                    }
                } else {
                    result.add(prior);
                    prior = current;
                }
            }
        }
        if (prior != null) {
            result.add(prior);
        }
        return result;
    }


    /**
     * description: 合并确定停留点
     * param: [rows, moveList]
     * return: scala.Tuple2<java.util.List<org.apache.spark.sql.Row>,java.util.List<org.apache.spark.sql.Row>>
     **/
    private static Tuple2<List<Row>, List<Row>> mergeStayPoint(List<Row> rows, List<Row> moveList) {
        List<Row> result = Lists.newArrayList();
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if ((Byte) current.getAs("point_type") == MOVE_POINT) {
                moveList.add(current);
                continue;
            }
            if (prior == null) {
                prior = current;
            } else {
                int distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) prior.getAs("lng"), (double) prior.getAs("lat"));
                if ((Byte) prior.getAs("point_type") == STAY_POINT && (Byte) current.getAs("point_type") == STAY_POINT && distance <= RANGE_I) {
                    int moveTime1 = prior.getAs("move_time");
                    int moveTime2 = current.getAs("move_time");
                    //两点合并
                    if (moveTime1 >= moveTime2) {
                        if (i + 1 < rows.size()) {
                            prior = calcRow(prior, rows.get(i + 1), (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        } else {
                            prior = calcRow(prior, null, (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        }
                    } else {
                        if (i + 1 < rows.size()) {
                            prior = calcRow(current, rows.get(i + 1), (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        } else {
                            prior = calcRow(current, null, (Timestamp) prior.getAs("begin_time"), (Timestamp) current.getAs("last_time"));
                        }
                    }
                } else {
                    result.add(prior);
                    prior = current;
                }
            }
        }
        if (prior != null) {
            result.add(prior);
        }
        return new Tuple2<>(result, moveList);
    }

    /**
     * description: 查找当前非确定停留点附近的确定停留点，先找前面最近的，后找后面最近的
     * param: [rows, pos]
     * return: org.apache.spark.sql.Row
     **/
    private static Row getNearestStayPoint(List<Row> rows, int pos) {
        for (int i = pos - 1; i >= 0; i--) {
            Row result = rows.get(i);
            byte pointType = result.getAs("point_type");
            if (pointType == STAY_POINT) {
                return result;
            }
        }
        for (int i = pos + 1; i < rows.size(); i++) {
            Row result = rows.get(i);
            byte pointType = result.getAs("point_type");
            if (pointType == STAY_POINT) {
                return result;
            }
        }
        return null;
    }

    /**
     * description: 重新判断可能停留点状态
     * param: [rows]
     * return: java.util.List<org.apache.spark.sql.Row>
     **/
    private static List<Row> mergeUncertainPoint(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row nearest_stayPoint = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if ((Byte) current.getAs("point_type") == STAY_POINT) {
                nearest_stayPoint = current;
                result.add(current);
            } else if (nearest_stayPoint == null) {
                Row stayPointRow = getNearestStayPoint(rows, i);
                byte newType = MOVE_POINT;
                if (stayPointRow != null) {
                    int distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) stayPointRow.getAs("lng"), (double) stayPointRow.getAs("lat"));
                    if (distance > RANGE_II) {
                        newType = STAY_POINT;
                        nearest_stayPoint = new GenericRowWithSchema(new Object[]{current.getAs("date"), current.getAs("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"),
                                current.getAs("begin_time"), current.getAs("last_time"), current.getAs("distance"), current.getAs
                                ("move_time"), current.getAs("speed"), newType}, ODSchemaProvider.TRACE_SCHEMA);
                    }
                }
                result.add(new GenericRowWithSchema(new Object[]{current.getAs("date"), current.getAs("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"),
                        current.getAs("begin_time"), current.getAs("last_time"), current.getAs("distance"), current.getAs("move_time"), current.getAs("speed"), newType}, ODSchemaProvider
                        .TRACE_SCHEMA));

            } else {
                int distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) nearest_stayPoint.getAs("lng"), (double) nearest_stayPoint.getAs("lat"));
                if (distance > RANGE_II) {
                    Row temp = new GenericRowWithSchema(new Object[]{current.getAs("date"), current.getAs("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"),
                            current.getAs("begin_time"), current.getAs("last_time"), current.getAs("distance"), current.getAs("move_time"), current.getAs("speed"), STAY_POINT}, ODSchemaProvider
                            .TRACE_SCHEMA);
                    nearest_stayPoint = temp;
                    result.add(temp);
                } else {
                    result.add(new GenericRowWithSchema(new Object[]{current.getAs("date"), current.getAs("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"),
                            current.getAs("begin_time"), current.getAs("last_time"), current.getAs("distance"), current.getAs("move_time"), current.getAs("speed"), MOVE_POINT}, ODSchemaProvider
                            .TRACE_SCHEMA));
                }
            }
        }
        return result;
    }

    /**
     * description: 计算两个时间直接的秒数差
     * param: [t1, t2]
     * return: int
     **/
    private static int getTimeDiff(Timestamp t1, Timestamp t2) {
        // t2 - t1
        return Seconds.secondsBetween(new DateTime(t1), new DateTime(t2)).getSeconds();
    }

    private static List<Row> timeAmendment(List<Row> stayPoints, List<Row> movePoints) {
        List<Row> result = new ArrayList<>();
        Row prev = null;
        int j = 0; // move points的指针
        for (int i = 0; i < stayPoints.size(); i++) {
            Row current = stayPoints.get(i);
            if (prev == null) {
                prev = current;
            } else {
                // 在处理OD对之前先把前面的位移点加入结果
                while (j < movePoints.size() && getTimeDiff((Timestamp) movePoints.get(j).getAs("last_time"), (Timestamp) prev.getAs("begin_time")) >= 0) {
                    result.add(movePoints.get(j));
                    j++;
                }
                Timestamp prevEnd = prev.getAs("last_time");
                Timestamp currentBegin = current.getAs("begin_time");
                //处理已经合并的停留点：删除合并前中间的位移点
                while (j < movePoints.size() && getTimeDiff((Timestamp) movePoints.get(j).getAs("begin_time"), (Timestamp) prev.getAs("last_time")) > 0) {
                    j++;
                }
                // 合并点
                int moveStart = j;
                int newPrev = -1;
                int newCur = -1;
                Row o_max = null;
                Row d_min = null;
                Row first = null; //newPrev的后一个row
                while (j < movePoints.size() && getTimeDiff((Timestamp) movePoints.get(j).getAs("last_time"), (Timestamp) current.getAs("begin_time")) >= 0) {
                    Row moveCurrent = movePoints.get(j);
                    if (prev.getAs("lng").equals(moveCurrent.getAs("lng")) && prev.getAs("lat").equals(moveCurrent.getAs("lat"))) {
                        o_max = moveCurrent;
                        newPrev = j;
                        if (j + 1 < movePoints.size() && getTimeDiff((Timestamp) movePoints.get(j + 1).getAs("last_time"), (Timestamp) current.getAs("begin_time")) >= 0) {
                            first = movePoints.get(j + 1);
                        }
                    }
                    if (d_min == null && current.getAs("lng").equals(moveCurrent.getAs("lng")) && current.getAs("lat").equals(moveCurrent.getAs("lat"))) {
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
                if (j - moveStart == 0 || newCur == moveStart || (d_min == null && newPrev == j - 1) || getTimeDiff(prevEnd, currentBegin) <= 0) {
                    int distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) prev.getAs("lng"), (double) prev.getAs("lat"));
                    int move_time = getTimeDiff((Timestamp) prev.getAs("begin_time"), currentBegin);
                    result.add(new GenericRowWithSchema(new Object[]{prev.getAs("date"), prev.getAs("msisdn"), prev.getAs("base"),
                            prev.getAs("lng"), prev.getAs("lat"), prev.getAs("begin_time"), prevEnd, distance, move_time,
                            MapUtil.formatDecimal(move_time == 0 ? 0 : (double) distance / move_time * 3.6, 2), prev.getAs("point_type")}, ODSchemaProvider.TRACE_SCHEMA));

                } else {
                    // 重新计算时间
                    first = first == null ? movePoints.get(moveStart) : first;
                    Timestamp moveBegin = first.getAs("begin_time");
                    int distance = MapUtil.getDistance((double) prev.getAs("lng"), (double) prev.getAs("lat"), (double) first.getAs("lng"), (double) first.getAs("lat"));
                    if (getTimeDiff(prevEnd, moveBegin) >= 600) {
                        int delta = Math.round(distance * 3.6f / 8);
                        Timestamp newEnd = new Timestamp(moveBegin.getTime() - delta * 1000);
                        prevEnd = getTimeDiff(prevEnd, newEnd) > 0 ? newEnd : prevEnd;
                    }

                    int move_time = getTimeDiff((Timestamp) prev.getAs("begin_time"), moveBegin);
                    result.add(new GenericRowWithSchema(new Object[]{prev.getAs("date"), prev.getAs("msisdn"), prev.getAs("base"),
                            prev.getAs("lng"), prev.getAs("lat"), prev.getAs("begin_time"), prevEnd, distance, move_time,
                            MapUtil.formatDecimal(move_time == 0 ? 0 : (double) distance / move_time * 3.6, 2), prev.getAs("point_type")}, ODSchemaProvider.TRACE_SCHEMA));

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
                prev = new GenericRowWithSchema(new Object[]{current.getAs("date"), current.getAs("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"),
                        currentBegin, current.getAs("last_time"), current.getAs("distance"), current.getAs("move_time"), current.getAs("speed"), current.getAs("point_type")}, ODSchemaProvider
                        .TRACE_SCHEMA);
            }
        }
        // 最后的处理
        if (prev != null) {
            result.add(prev);
            while (j < movePoints.size() && getTimeDiff((Timestamp) movePoints.get(j).getAs("begin_time"), (Timestamp) prev.getAs("last_time")) > 0) {
                j++;
            }
        }
        while (j < movePoints.size()) {
            result.add(movePoints.get(j));
            j++;
        }

        return result;
    }

    private static List<Row> addToList(int start, int end, List<Row> list, List<Row> result) {
        for (int i = start; i < end; i++) {
            result.add(list.get(i));
        }
        return result;
    }

    /**
    * description: 计算OD出行链，删除出行时间小于4分钟的OD出行，删除中间没有位移点并且出行时间大于等于40分钟的OD出行，记录每个OD对的最大速度、曲线距离、以及速度变异系数
     * 注意：如果 A-B 满足上述条件，也要把B-A的出行删除
    * param: [rows]
    * return: java.util.List<org.apache.spark.sql.Row>
    **/
    private static Tuple2<List<Row>,List<Row>> filterOD(List<Row> rows) {
        // 保存OD出行链信息，包括中间的点,key为 开始基站、结束基站、开始时间, value为 整个出行链信息
        Map<Tuple3<String,String, Timestamp>, List<Row>> odTraceMap = new HashMap<>();
        List<Row> odResult = new ArrayList<>();
        if(rows.size() < 2) {
            return new Tuple2<>(odResult, odResult);
        }
        // 当前记录
        Row current;
        // 前一条记录
        Row prior = null;
        LinkedList<Row> linked = new LinkedList<>();
        for(Row row: rows) {
            if ((Byte)row.getAs("point_type") == STAY_POINT) {
                current = row;
                if(prior == null) {
                    prior = current;
                }
                // 加入o点
                if(linked.size() == 0) {
                    linked.add(prior);
                } else if(linked.size() >= 1) { // 加入D点，并保存OD信息到Map中
                    linked.add(current);
                    String startBase = prior.getAs("base");
                    String endBase = current.getAs("base");
                    Timestamp beginTime = prior.getAs("begin_time");
                    odTraceMap.put(new Tuple3<>(startBase, endBase, beginTime), linked);

                    // 重置，开始下一个OD
                    prior = current;
                    linked = new LinkedList<>();
                    linked.add(prior);
                }
            } else {
                // 加入O和D中间的点
                if(linked.size()> 0) {
                    linked.add(row);
                }
            }
        }

        List<Tuple2<String, String>> shouldRemoveOD = new ArrayList<>();
        //List保存从小区到小区移动记录，包括O和D中间的位移点
        List<Row> traceOD = new ArrayList<>();
        for(Tuple3<String, String, Timestamp> tuple3: odTraceMap.keySet()) {
            LinkedList<Row> trace = (LinkedList<Row>)odTraceMap.get(tuple3);
            if(shouldRemoveOD.contains(new Tuple2<>(tuple3._1(), tuple3._2())) || shouldRemoveOD.contains(new Tuple2<>(tuple3._2(), tuple3._1()))){
                continue;
            }
            Row o = trace.peekFirst();
            Row d = trace.peekLast();
            Timestamp originEnd = o.getAs("last_time");
            Timestamp destBegin = d.getAs("begin_time");
            int moveTime = getTimeDiff(originEnd, destBegin);
            if (moveTime <= 240 || (trace.size() == 2 && moveTime >= 2400)) {
                shouldRemoveOD.add(new Tuple2<>(tuple3._1(), tuple3._2()));
                shouldRemoveOD.add(new Tuple2<>(tuple3._2(), tuple3._1()));
                continue;
            } else {
                // 利用2*delta检测法，删除O和D之间的异常点
                int beforeSize, afterSize;
                do {
                    beforeSize = trace.size();
                    trace =  (LinkedList<Row>)removeExceptionPoint(trace);
                    afterSize = trace.size();
                } while (beforeSize - afterSize > 0);

                // 获取曲线距离
                int linkedDistance = 0;
                for(Row row: trace) {
                    linkedDistance += (int)row.getAs("distance");
                }
                // 计算速度变异系数
                List<Double> speedList = new ArrayList<>();
                double sumSpeed = 0;

                for(Row row: trace) {
                    speedList.add((double)row.getAs("speed")) ;
                    sumSpeed += (double)row.getAs("speed");

                }
                // 最大速度
                double maxSpeed = speedList.get(0);
                for(Double speed: speedList) {
                    maxSpeed = maxSpeed > speed ? maxSpeed : speed;
                }

                Double[] speedArray = speedList.toArray(new Double[speedList.size()]);
                // 平均速度
                double avgSpeed = sumSpeed/speedList.size();
                // 速度变异系数
                Double covSpeed = 0d;
                try {
                    covSpeed = MapUtil.formatDecimal(MathUtil.variance(speedArray)/avgSpeed, 2);
                } catch (Exception ex) {

                }
                int distance = MapUtil.getDistance((double) o.getAs("lng"), (double) o.getAs("lat"), (double) d.getAs("lng"), (double) d.getAs("lat"));

                Row od = new GenericRowWithSchema(new Object[]{o.getAs("date"), o.getAs("msisdn"), o.getAs("base"),
                        o.getAs("lng"), o.getAs("lat"), d.getAs("base"), d.getAs("lng"), d.getAs("lat"),
                        originEnd, destBegin, linkedDistance, maxSpeed, covSpeed, distance, moveTime}, ODSchemaProvider.OD_SCHEMA);
                odResult.add(od);
                if(trace.size()>1) {
                    traceOD.addAll(createODTrace(trace));
                }
            }
        }
        return new Tuple2<>(odResult, traceOD);
    }

    /**
    * description: 创建小区到小区之间的出行OD，包括位移点，用于统计小区到小区的出行数据
    * param: [trace]
    * return: java.util.List<org.apache.spark.sql.Row>
    **/
    private static List<Row> createODTrace(List<Row> trace) {
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("begin_time");
            }
        });
        // 重新按照时间排序
        trace = ordering.sortedCopy(trace);
        Row prior = null;
        Row current ;
        List<Row> odTraceList = new ArrayList<>();
        for(Iterator<Row> it = trace.iterator(); it.hasNext();) {
            Row row = it.next();
            current = row;
            if(prior == null) {
                prior = current;
            } else {
//                Timestamp originEnd = prior.getAs("last_time");
//                Timestamp destBegin = current.getAs("begin_time");
                Row odTrace = new GenericRowWithSchema(new Object[]{prior.getAs("date"), prior.getAs("msisdn"), prior.getAs("base"),
                        prior.getAs("lng"), prior.getAs("lat"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"),
                        prior.getAs("last_time"), current.getAs("begin_time")}, ODSchemaProvider.OD_TRACE_SCHEMA);
                odTraceList.add(odTrace);
                // 重置开始
                prior = current;
            }
        }
        return odTraceList;
    }

    /**
    * description: 通过2*delta方法删除OD之间的异常点，具体要求参考《手机信令预处理xxx》中的 删除异常数据
    * param: [trace]
    * return: java.util.LinkedList<org.apache.spark.sql.Row>
    **/
    private static List<Row> removeExceptionPoint(List<Row> trace) {
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("begin_time");
            }
        });
        trace = ordering.sortedCopy(trace);
        // 计算平均距离、距离标准差和平均速度、速度标准差
        int sumDistance = 0;
        List<Double> distanceList = new ArrayList<>();
        for(Row row: trace) {
            Double tmpdis = Double.valueOf((int)row.getAs("distance"));
            distanceList.add(tmpdis);
            sumDistance += (int)row.getAs("distance");

        }
        double avgDistance = MapUtil.formatDecimal((double) sumDistance/trace.size(), 2);
        double stdDistance = MathUtil.stdVariance(distanceList.toArray(new Double[distanceList.size()]));


        List<Double> speedList = new ArrayList<>();
        double sumSpeed = 0;
        for(Row row: trace) {
            speedList.add((double)row.getAs("speed"));
            sumSpeed += (double)row.getAs("speed");

        }
        double avgSpeed = MapUtil.formatDecimal((double) sumSpeed/trace.size(), 2);
        double stdSpeed = MathUtil.stdVariance(speedList.toArray(new Double[speedList.size()]));


        List<Row> result = new LinkedList<>();
        Row prior = null;
        Row current ;
        for(Row row: trace) {
            current = row;
            if(prior == null) {
                prior = current;
            } else {
                Timestamp originEnd = prior.getAs("last_time");
                Timestamp destBegin = current.getAs("begin_time");

                // D点之间加入
                if ((Byte)current.getAs("point_type") == STAY_POINT) {
                    result.add(calcRow(prior, current, originEnd, destBegin));
                    result.add(current);
                    continue;
                }

                int distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) prior.getAs("lng"), (double) prior.getAs("lat"));
                double subDistance = Math.abs(distance - avgDistance);

                int timeDiff = getTimeDiff(originEnd, destBegin);
                double speed = MapUtil.formatDecimal(timeDiff == 0 ? 0 : (double) distance / timeDiff * 3.6, 2);
                double subSpeed = Math.abs(speed - avgSpeed);
                if(subDistance <= 2 * stdDistance && subSpeed <= 2 * stdSpeed) {
                    result.add(calcRow(prior, current, originEnd, destBegin));
                    prior = current;
                } else {
                    continue;
                }
            }

        }
        return result;
    }

    public void process(String validSignalFile) {
        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }

        DataFrame df = signalLoader.load(validSignalFile);
        //手机号码->信令数据
        JavaPairRDD<String, Row> rdd1 = df.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String msisdn = row.getAs("msisdn");
                Integer date = row.getAs("date");
                return new Tuple2<>(msisdn + "|" + date, row);
            }
        });

        //将同一手机号码的信令数据聚合到一个List中, 重新分区
        List<Row> rows = Lists.newArrayList();
        JavaPairRDD<String, List<Row>> rdd2 = rdd1.aggregateByKey(rows, params.getPartitions(), new Function2<List<Row>, Row, List<Row>>() {
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
        JavaRDD<Tuple2<List<Row>, List<Row>>> rdd3 = rdd2.values().map(new Function<List<Row>, Tuple2<List<Row>, List<Row>>>() {
            @Override
            public Tuple2<List<Row>, List<Row>> call(List<Row> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
                    @Override
                    public Timestamp apply(Row row) {
                        return row.getAs("begin_time");
                    }
                });

                rows = determinePointType(rows);

                //按startTime排序
                rows = ordering.sortedCopy(rows);
                // 连续确定停留点和可能停留点合并
                int beforeSize, afterSize;
                do {
                    beforeSize = rows.size();
                    rows = mergeContinuePoint(rows);
                    afterSize = rows.size();
                } while (beforeSize - afterSize > 0);

                // 单独抽出确定停留点和可能停留点，合并连续确定停留点
                Tuple2<List<Row>, List<Row>> pointSplit = mergeStayPoint(rows, new ArrayList<Row>());
                rows = pointSplit._1();
                List<Row> moveList = pointSplit._2();
                // 判断可能停留点状态
                rows = mergeUncertainPoint(rows);
                // 循环至所有确定停留点间距 > RANGE_I
                do {
                    beforeSize = rows.size();
                    pointSplit = mergeStayPoint(rows, moveList);
                    rows = pointSplit._1();
                    moveList = pointSplit._2();
                    afterSize = rows.size();
                } while (beforeSize - afterSize > 0);

                moveList = ordering.sortedCopy(moveList);
                rows = ordering.sortedCopy(rows);
                rows = timeAmendment(rows, moveList);
                Tuple2<List<Row>, List<Row>> result = filterOD(rows);
                // 有效的ODTrace，包括中间的位移点
                List<Row> odTrace = result._2;
                // OD数据，只包含每次出行的O和D
                List<Row> od = result._1;
                return new Tuple2<>(odTrace, od);
            }
        });

        rdd3 = rdd3.persist(StorageLevel.DISK_ONLY());

        JavaRDD<Row> rdd4 = rdd3.flatMap(new FlatMapFunction<Tuple2<List<Row>, List<Row>>, Row>() {
            @Override
            public Iterable<Row> call(Tuple2<List<Row>, List<Row>> result) throws Exception {
                return result._1();
            }
        });
        df = sqlContext.createDataFrame(rdd4, ODSchemaProvider.OD_TRACE_SCHEMA);
        df = df.orderBy("msisdn", "leave_time");
        String date = df.first().getAs("date").toString();
        FileUtil.saveFile(df.repartition(partitions), FileUtil.FileType.CSV, params.getSavePath() + "od_trace/" + date);

        JavaRDD<Row> odRDD = rdd3.flatMap(new FlatMapFunction<Tuple2<List<Row>, List<Row>>, Row>() {
            @Override
            public Iterable<Row> call(Tuple2<List<Row>, List<Row>> result) throws Exception {
                return result._2();
            }
        });
        DataFrame odResultDF = sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_SCHEMA);
        FileUtil.saveFile(odResultDF.repartition(partitions), FileUtil.FileType.CSV, params.getSavePath() + "od/" + date);
        rdd3.unpersist();
    }
}

