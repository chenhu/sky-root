package com.sky.signal.pre.util;

import com.google.common.collect.Lists;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chenhu on 2020/6/17.
 * 停留点分析公用部分方法
 */
@Slf4j
@Component
public class StayPointUtil implements Serializable {

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

    /**
     * description: 根据逗留时间和速度，增加每行的停留点类型
     * param: [rows]
     * return: java.util.List<org.apache.spark.sql.Row>
     **/
    public List<Row> determinePointType(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(new GenericRowWithSchema(new Object[]{row.getAs
                    ("date"), row.getAs("msisdn"), row.getAs("base"), row
                    .getAs("lng"), row.getAs("lat"), row.getAs("begin_time"),
                    row.getAs("last_time"), row.getAs("distance"), row.getAs
                    ("move_time"), row.getAs("speed"), getPointType((Integer) row
                    .getAs("move_time"), (Double) row.getAs("speed"))},
                    ODSchemaProvider.TRACE_SCHEMA));
        }
        return result;
    }

    /**
     * description: 根据逗留时间和速度，返回停留点类型
     * param: [moveTime, speed]
     * return: byte
     **/
    private byte getPointType(int moveTime, double speed) {
        byte pointType = SignalProcessUtil.MOVE_POINT;
        if (moveTime >= STAY_TIME_MAX) {
            pointType = SignalProcessUtil.STAY_POINT;
        } else if (moveTime >= STAY_TIME_MIN && moveTime < STAY_TIME_MAX &&
                speed < MOVE_SPEED) {
            pointType = SignalProcessUtil.UNCERTAIN_POINT;
        }
        return pointType;
    }

    /**
     * description: 计算两个移动点直接的距离、速度、和停留点类型
     * param: [prior, current, startTime, lastTime]
     * return: org.apache.spark.sql.Row
     **/
    public Row getDistanceSpeedMovetime(Row prior, Row current, Timestamp
            startTime, Timestamp lastTime) {
        int distance = 0;
        int moveTime = (int) (lastTime.getTime() - startTime.getTime()) / 1000;
        double speed = 0d;
        byte pointType = (Byte) prior.getAs("point_type");
        if (prior != current && current != null) {
            //基站与下一基站距离
            distance = MapUtil.getDistance((Double) current.getAs("lng"),
                    (Double) current.getAs("lat"), (Double) prior.getAs
                            ("lng"), (Double) prior.getAs("lat"));
            //基站移动到下一基站时间 = 下一基站startTime - 基站startTime
            moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                    .getAs("begin_time")), new DateTime(prior.getAs
                    ("begin_time"))).getSeconds());
            //基站移动到下一基站速度
            speed = MapUtil.formatDecimal(moveTime == 0 ? 0 : distance / moveTime * 3.6, 2);
        }
        pointType = pointType == SignalProcessUtil.UNCERTAIN_POINT ?
                getPointType(moveTime, speed) : pointType;
        return new GenericRowWithSchema(new Object[]{prior.getAs("date"),
                prior.getAs("msisdn"), prior.getAs("base"), prior.getAs
                ("lng"), prior.getAs("lat"), startTime, lastTime, distance,
                moveTime, speed, pointType}, ODSchemaProvider.TRACE_SCHEMA);
    }

    /**
     * description: 合并连续的停留点和可能停留点
     * param: [rows]
     * return: java.util.List<org.apache.spark.sql.Row>
     **/
    public List<Row> mergeContinuePoint(List<Row> rows) {
        List<Row> result = Lists.newArrayList();
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if (prior == null) {
                prior = current;
            } else {
                byte priorType = (Byte)prior.getAs("point_type");
                byte currentType = (Byte)current.getAs("point_type");
                int distance = MapUtil.getDistance((Double) current.getAs
                        ("lng"), (Double) current.getAs("lat"), (Double)
                        prior.getAs("lng"), (Double) prior.getAs("lat"));
                if ((priorType == SignalProcessUtil.STAY_POINT && currentType == SignalProcessUtil.STAY_POINT && distance <= RANGE_I)
                        || (((priorType == SignalProcessUtil.STAY_POINT && currentType == SignalProcessUtil.UNCERTAIN_POINT)
                        || (priorType == SignalProcessUtil.UNCERTAIN_POINT && currentType == SignalProcessUtil.STAY_POINT)) && distance <= RANGE_II)
                        || (priorType == SignalProcessUtil.UNCERTAIN_POINT && currentType == SignalProcessUtil.UNCERTAIN_POINT && distance <= RANGE_III)) {
                    int moveTime1 = (Integer)prior.getAs("move_time");
                    int moveTime2 = (Integer)current.getAs("move_time");
                    //两点合并
                    if (moveTime1 >= moveTime2) {
                        if (i + 1 < rows.size()) {
                            prior = getDistanceSpeedMovetime(prior, rows.get
                                    (i + 1), (Timestamp) prior.getAs
                                    ("begin_time"), (Timestamp) current.getAs
                                    ("last_time"));
                        } else {
                            prior = getDistanceSpeedMovetime(prior, null,
                                    (Timestamp) prior.getAs("begin_time"),
                                    (Timestamp) current.getAs("last_time"));
                        }
                    } else {
                        if (i + 1 < rows.size()) {
                            prior = getDistanceSpeedMovetime(current, rows
                                    .get(i + 1), (Timestamp) prior.getAs
                                    ("begin_time"), (Timestamp) current.getAs
                                    ("last_time"));
                        } else {
                            prior = getDistanceSpeedMovetime(current, null,
                                    (Timestamp) prior.getAs("begin_time"),
                                    (Timestamp) current.getAs("last_time"));
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
     * return: scala.Tuple2<java.util.List<org.apache.spark.sql
     * .Row>,java.util.List<org.apache.spark.sql.Row>>
     **/
    public Tuple2<List<Row>, List<Row>> mergeStayPoint(List<Row> rows,
                                                       List<Row> moveList) {
        List<Row> result = Lists.newArrayList();
        Row prior = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if ((Byte) current.getAs("point_type") == SignalProcessUtil
                    .MOVE_POINT) {
                moveList.add(current);
                continue;
            }
            if (prior == null) {
                prior = current;
            } else {
                int distance = MapUtil.getDistance((Double) current.getAs
                        ("lng"), (Double) current.getAs("lat"), (Double)
                        prior.getAs("lng"), (Double) prior.getAs("lat"));
                if ((Byte) prior.getAs("point_type") == SignalProcessUtil
                        .STAY_POINT && (Byte) current.getAs("point_type") ==
                        SignalProcessUtil.STAY_POINT && distance <= RANGE_I) {
                    int moveTime1 = (Integer) prior.getAs("move_time");
                    int moveTime2 = (Integer)current.getAs("move_time");
                    //两点合并
                    if (moveTime1 >= moveTime2) {
                        if (i + 1 < rows.size()) {
                            prior = getDistanceSpeedMovetime(prior, rows.get
                                    (i + 1), (Timestamp) prior.getAs
                                    ("begin_time"), (Timestamp) current.getAs
                                    ("last_time"));
                        } else {
                            prior = getDistanceSpeedMovetime(prior, null,
                                    (Timestamp) prior.getAs("begin_time"),
                                    (Timestamp) current.getAs("last_time"));
                        }
                    } else {
                        if (i + 1 < rows.size()) {
                            prior = getDistanceSpeedMovetime(current, rows
                                    .get(i + 1), (Timestamp) prior.getAs
                                    ("begin_time"), (Timestamp) current.getAs
                                    ("last_time"));
                        } else {
                            prior = getDistanceSpeedMovetime(current, null,
                                    (Timestamp) prior.getAs("begin_time"),
                                    (Timestamp) current.getAs("last_time"));
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
    private Row getNearestStayPoint(List<Row> rows, int pos) {
        for (int i = pos - 1; i >= 0; i--) {
            Row result = rows.get(i);
            byte pointType = (Byte)result.getAs("point_type");
            if (pointType == SignalProcessUtil.STAY_POINT) {
                return result;
            }
        }
        for (int i = pos + 1; i < rows.size(); i++) {
            Row result = rows.get(i);
            byte pointType = (Byte)result.getAs("point_type");
            if (pointType == SignalProcessUtil.STAY_POINT) {
                return result;
            }
        }
        return null;
    }

    /**
     * description: 合并可能停留点
     * param: [rows]
     * return: java.util.List<org.apache.spark.sql.Row>
     **/
    public List<Row> mergeUncertainPoint(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row nearest_stayPoint = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if ((Byte) current.getAs("point_type") == SignalProcessUtil
                    .STAY_POINT) {
                nearest_stayPoint = current;
                result.add(current);
            } else if (nearest_stayPoint == null) {
                Row stayPointRow = getNearestStayPoint(rows, i);
                byte newType = SignalProcessUtil.MOVE_POINT;
                if (stayPointRow != null) {
                    int distance = MapUtil.getDistance((Double) current.getAs
                            ("lng"), (Double) current.getAs("lat"), (Double)
                            stayPointRow.getAs("lng"), (Double) stayPointRow
                            .getAs("lat"));
                    if (distance > RANGE_II) {
                        newType = SignalProcessUtil.STAY_POINT;
                        nearest_stayPoint = new GenericRowWithSchema(new
                                Object[]{current.getAs("date"), current.getAs
                                ("msisdn"), current.getAs("base"), current
                                .getAs("lng"), current.getAs("lat"), current
                                .getAs("begin_time"), current.getAs
                                ("last_time"), current.getAs("distance"),
                                current.getAs("move_time"), current.getAs
                                ("speed"), newType}, ODSchemaProvider
                                .TRACE_SCHEMA);
                    }
                }
                result.add(new GenericRowWithSchema(new Object[]{current
                        .getAs("date"), current.getAs("msisdn"), current
                        .getAs("base"), current.getAs("lng"), current.getAs
                        ("lat"), current.getAs("begin_time"), current.getAs
                        ("last_time"), current.getAs("distance"), current
                        .getAs("move_time"), current.getAs("speed"),
                        newType}, ODSchemaProvider.TRACE_SCHEMA));

            } else {
                int distance = MapUtil.getDistance((Double) current.getAs
                        ("lng"), (Double) current.getAs("lat"), (Double)
                        nearest_stayPoint.getAs("lng"), (Double)
                        nearest_stayPoint.getAs("lat"));
                if (distance > RANGE_II) {
                    Row temp = new GenericRowWithSchema(new Object[]{current
                            .getAs("date"), current.getAs("msisdn"), current
                            .getAs("base"), current.getAs("lng"), current
                            .getAs("lat"), current.getAs("begin_time"),
                            current.getAs("last_time"), current.getAs
                            ("distance"), current.getAs("move_time"), current
                            .getAs("speed"), SignalProcessUtil.STAY_POINT},
                            ODSchemaProvider.TRACE_SCHEMA);
                    nearest_stayPoint = temp;
                    result.add(temp);
                } else {
                    result.add(new GenericRowWithSchema(new Object[]{current
                            .getAs("date"), current.getAs("msisdn"), current
                            .getAs("base"), current.getAs("lng"), current
                            .getAs("lat"), current.getAs("begin_time"),
                            current.getAs("last_time"), current.getAs
                            ("distance"), current.getAs("move_time"), current
                            .getAs("speed"), SignalProcessUtil.MOVE_POINT},
                            ODSchemaProvider.TRACE_SCHEMA));
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
    public int getTimeDiff(Timestamp t1, Timestamp t2) {
        // t2 - t1
        return Seconds.secondsBetween(new DateTime(t1), new DateTime(t2))
                .getSeconds();
    }
}
