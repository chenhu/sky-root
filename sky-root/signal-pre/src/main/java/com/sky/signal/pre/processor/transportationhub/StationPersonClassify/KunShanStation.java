package com.sky.signal.pre.processor.transportationhub.StationPersonClassify;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.processor.transportationhub.PersonClassic;
import com.sky.signal.pre.util.MapUtil;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Chenhu on 2020/5/27.
 * 昆山站人口分类器
 * 针对逗留时间小于60分钟的情况
 * <p>
 * 针对当天逗留时间 < 60min的用户（同一MSISDN），按照字段（start_time）排序，得到其轨迹
 * ，找出其中的枢纽点Pj，若Pj的start_time在0:00 – 0:35之间，且Pj的move_time >=
 * 3min，且在Pj之前（P1,
 * P2,…,Pj-1之中）有停留点或该用户在前一天有停留点，则判断该用户为铁路出发人口；若Pj的start_time在22:30 –
 * 24:00之间，且Pj的move_time >= 76s，且在Pj之后（Pj+1, …,
 * Pn之中）有停留点或该用户在后一天有停留点，则判断该用户为铁路到达人口；其他用户则判断为铁路过境人口
 */
@Component
public class KunShanStation implements Serializable {
    private static final Integer THREE_MI = 3 * 60;
    private static final Integer FIVE_MI = 5 * 60;
    private static final Integer TWO_HOUR = 2 * 60 * 60;
    private static final Integer SEVENTY_SIX = 76;
    private static final Integer TEN_MI = 10 * 60;
    DateTimeFormatter formatterDate = DateTimeFormat.forPattern("yyyyMMdd");
    DateTimeFormatter formatterDateTime = DateTimeFormat.forPattern
            ("yyyy-MM-dd HH:mm:ss.S");
    DateTimeFormatter formatterDateTimeMi = DateTimeFormat.forPattern
            ("yyyyMMddHHmm");
    @Autowired
    private transient ParamProperties params;

    public List<Row> classify(List<Row> rows, DataFrame stationTrace) {
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return (Timestamp) row.getAs("begin_time");
            }
        });
        //按startTime排序
        rows = ordering.sortedCopy(rows);
        rows = getClassic(rows, stationTrace);
        return rows;
    }

    private List<Row> getClassic(List<Row> rows, DataFrame stationTrace) {
        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());
        //默认为枢纽过境人口
        Integer personClassic = PersonClassic.TransportationHubPassBy
                .getIndex();

        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            Timestamp beginTime = (Timestamp) row.getAs("begin_time");
            Integer date = (Integer) row.getAs("date");
            Integer moveTime = (Integer) row.getAs("move_time");

            //今天的日期
            LocalDate today = LocalDate.parse(date.toString(), formatterDate);
            //昨天的日期
            Integer yesterday = Integer.valueOf(today.minusDays(1).toString
                    ("yyyyMMdd"));
            //明天的日期
            Integer tomorrow = Integer.valueOf(today.plusDays(1).toString
                    ("yyyyMMdd"));
            //手机号码
            String msisdn = (String) row.getAs("msisdn");


            LocalDateTime localDateTime1 = LocalDateTime.parse(date.toString
                    () + "0000", formatterDateTimeMi);
            LocalDateTime localDateTime2 = LocalDateTime.parse(date.toString
                    () + "0035", formatterDateTimeMi);
            LocalDateTime localDateTime3 = LocalDateTime.parse(date.toString
                    () + "2230", formatterDateTimeMi);
            LocalDateTime localDateTime4 = LocalDateTime.parse(date.toString
                    () + "2359", formatterDateTimeMi);

            String base = (String) row.getAs("base");
            // 如果是枢纽基站
            if (base.equals(params.getVisualStationBase())) {
                LocalDateTime beginTimeLocalDateTime = LocalDateTime.parse
                        (beginTime.toString(), formatterDateTime);
                // 在00:00-00:35之间并且停留时间大于等于3分钟
                if (beginTimeLocalDateTime.isBefore(localDateTime2) &&
                        beginTimeLocalDateTime.isAfter(localDateTime1) &&
                        moveTime >= THREE_MI) {
                    //判断在当前记录之前是否有停留点
                    Boolean hasStayPoint = false;
                    for (int j = 0; j < i; j++) {
                        byte tmpPointType = (Byte) rows.get(j).getAs("point_type");
                        if (tmpPointType == SignalProcessUtil.STAY_POINT) {
                            hasStayPoint = true;
                            break;
                        }
                    }
                    if (hasStayPoint) { // 枢纽站之前有停留点，则为枢纽站出发人口
                        personClassic = PersonClassic.Leave.getIndex();
                        //判断出最终结果，结束循环
                        break;
                    } else {//判断昨天是否有停留点
                        DataFrame yesterDayDf = stationTrace.filter(col
                                ("date").equalTo(yesterday)).filter(col
                                ("msisdn").equalTo(msisdn)).filter(col
                                ("point_type").equalTo(SignalProcessUtil
                                .STAY_POINT));
                        //昨天基站轨迹信令中有停留点,结束循环
                        if (yesterDayDf.count() > 0L) {
                            personClassic = PersonClassic.Leave.getIndex();
                            break;

                        }

                    }
                } else if (beginTimeLocalDateTime.isBefore(localDateTime4) &&
                        beginTimeLocalDateTime.isAfter(localDateTime3) &&
                        moveTime >= SEVENTY_SIX) {//在22:30到23:59分之间并且停留时间大于等于76秒
                    //判断在当前记录之后是否有停留点
                    Boolean hasStayPoint = false;
                    for (int j = i + 1; j < rows.size(); j++) {
                        byte tmpPointType = (Byte)rows.get(j).getAs("point_type");
                        if (tmpPointType == SignalProcessUtil.STAY_POINT) {
                            hasStayPoint = true;
                            break;
                        }
                    }
                    if (hasStayPoint) { // 枢纽站之前有停留点，则为枢纽站出发人口
                        personClassic = PersonClassic.Arrive.getIndex();
                        //判断出最终结果，结束循环
                        break;
                    } else {//判断明天是否有停留点
                        DataFrame tomorrowDayDf = stationTrace.filter(col
                                ("date").equalTo(tomorrow)).filter(col
                                ("msisdn").equalTo(msisdn)).filter(col
                                ("point_type").equalTo(SignalProcessUtil
                                .STAY_POINT));
                        //明天基站轨迹信令中有停留点,结束循环
                        if (tomorrowDayDf.count() > 0) {
                            personClassic = PersonClassic.Arrive.getIndex();
                            break;

                        }
                    }

                }

            }
        }

        //根据人口分类，创建带人口分类的信令数据集合
        for (Row row : rows) {
            Row rowWithPersionClassic = new GenericRowWithSchema(new
                    Object[]{row.getAs("date"), row.getAs("msisdn"), row
                    .getAs("base"), row.getAs("lng"), row.getAs("lat"), row
                    .getAs("begin_time"), row.getAs("last_time"), row.getAs
                    ("distance"), row.getAs("move_time"), row.getAs("speed"),
                    row.getAs("point_type"), personClassic.byteValue()},
                    ODSchemaProvider.STATION_TRACE_CLASSIC_SCHEMA);
            resultList.add(rowWithPersionClassic);
        }

        return resultList;
    }

    /**
     * 如果有效枢纽站个数为1个的时候，结合当前分析枢纽的列车时刻表来判定人口分类
     * <pre>
     *     4)	若Y=1则定义枢纽站 前为{Q1,Q2,…,Qj-1}，枢纽站 后为{Qj+1,Qj+2,…,Qn}：
     * （1）如果  的start_time在<=0:35,(0:00 – 0:35]之间，且 的move_time >= 3min，且在
     * 之前{Q1,Q2,…,Qj-1}之中）有停留点或该用户在前一天有停留点，
     * ①若 之后无停留点，则判断该用户为铁路出发人口；
     * ②若 后有停留点且第一个停留点B满足 move_time >= 2hr && last_time – start_time <
     * 5min或者B满足 move_time >=
     * 2hr，且B与该MSID的就业地或居住地>800m的条件，则判断该用户为铁路出发人口（当天往返，但非火车返回），否则为城市途经人口。
     * （2）如果 的start_time在>=22:30,[22:30– 24:00)之间，且 的move_time >= 76s，且在
     * 之后{Qj+1,Qj+2,…,Qn}之中）有停留点或该用户在后一天有停留点，则判断该用户为铁路到达人口；否则为城市途经人口
     * （3）如果 的start_time在（0:35 – 22:30）之间
     * ①若枢纽站前有停留点而枢纽站后无停留点，若枢纽站逗留时间 > 3min，判断该用户为铁路出发人口；若枢纽站逗留时间 <=
     * 3min，判断该用户为非高铁出行对外交通人口；
     * ②若枢纽站前有停留点且枢纽站后也有停留点，若枢纽站逗留时间 > 3min且枢纽站后的第一个停留点B满足 move_time >= 2hr
     * && last_time – start_time < 5min或者 枢纽站后的第一个停留点B满足 move_time >=
     * 2hr，且B与该MSID的就业地或居住地>800m
     * 的条件，则判断该用户为铁路出发人口（当天往返，但非火车返回），若不满足上述条件或者枢纽站逗留时间 <= 3min，则判断该用户为城市途经人口；
     * ③若枢纽站前无停留点而枢纽站后有停留点，若枢纽站逗留时间 >= 76s，判断该用户为铁路到达人口；若枢纽站逗留时间 <
     * 76s，判断该用户为城市途经人口；
     * ④若枢纽站前无停留点且枢纽站后也无停留点，则判断该用户为城市途经人口。
     *
     * </pre>
     *
     * @param rows         用户一天的数据
     * @param stationTrace 枢纽站当前分析阶段所有用户数据，用于查找当前用户昨天和明天是否有停留点
     * @param workLiveDf 当前分析阶段所有数据计算出的用户职住信息
     * @return 增加了枢纽站人口分类的信令数据
     */
    public List<Row> oneStationBaseProc(List<Row> rows, DataFrame
            stationTrace, DataFrame workLiveDf) {
        //需要按照时间排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return (Timestamp) row.getAs("begin_time");
            }
        });
        //按begin_time排序
        rows = ordering.sortedCopy(rows);

        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());
        //枢纽站的行
        Row stationBaseRow = null;
        //枢纽站行的索引位置
        int stationBaseIndex = 0;
        //定义人口分类，默认为城市途经人口
        Integer personClassic = PersonClassic.CityPassBy.getIndex();
        //找到枢纽站位置
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).getAs("base").equals(params.getVisualStationBase
                    ())) {
                stationBaseRow = rows.get(i);
                stationBaseIndex = i;
                break;
            }
        }
        // 枢纽站前轨迹
        List<Row> preStationBaseList = rows.subList(0, stationBaseIndex);
        // 枢纽站后轨迹
        List<Row> behindStationBaseList = rows.subList(stationBaseIndex + 1,
                rows.size());
        //枢纽站前轨迹是否有确定停留点,枢纽站后轨迹是否有确定停留点
        boolean hasPreStayPoint = false, hasBehindStayPoint = false;
        //枢纽站后第一个的停留点的行
        Row behindStayPointRow = null;

        //枢纽站前是否有停留点
        for (Row row : preStationBaseList) {
            Byte stayPoint = (Byte) row.getAs("stay_point");
            if (stayPoint == SignalProcessUtil.STAY_POINT) {
                hasPreStayPoint = true;
                break;
            }
        }
        //枢纽站后是否有停留点
        for (Row row : behindStationBaseList) {
            Byte stayPoint = (Byte) row.getAs("stay_point");
            if (stayPoint == SignalProcessUtil.STAY_POINT) {
                hasBehindStayPoint = true;
                behindStayPointRow = row;
                break;
            }
        }

        //当天日期
        Integer date = (Integer) stationBaseRow.getAs("date");

        //今天的日期
        LocalDate today = LocalDate.parse(date.toString(), formatterDate);
        //昨天的日期
        Integer yesterday = Integer.valueOf(today.minusDays(1).toString
                ("yyyyMMdd"));
        //明天的日期
        Integer tomorrow = Integer.valueOf(today.plusDays(1).toString
                ("yyyyMMdd"));
        //手机号码
        String msisdn = (String) stationBaseRow.getAs("msisdn");

        Boolean yesterdayHasStayPoint = this.yesterdayHasStayPoint
                (stationTrace, yesterday, msisdn);
        Boolean tomorrowHasStayPoint = this.tomorrowHasStayPoint
                (stationTrace, tomorrow, msisdn);

        //根据枢纽站开始数据在列车时刻表的不同位置，进行人口分类
        //列车时刻表
        LocalDateTime localDateTime1 = LocalDateTime.parse(date.toString() +
                "0000", formatterDateTimeMi);
        LocalDateTime localDateTime2 = LocalDateTime.parse(date.toString() +
                "0035", formatterDateTimeMi);
        LocalDateTime localDateTime3 = LocalDateTime.parse(date.toString() +
                "2230", formatterDateTimeMi);
        LocalDateTime localDateTime4 = LocalDateTime.parse(date.toString() +
                "2359", formatterDateTimeMi);
        //枢纽站开始时间
        Timestamp stationBaseBeginTime = (Timestamp) stationBaseRow.getAs("begin_time");
        LocalDateTime beginTimeLocalDateTime = LocalDateTime.parse
                (stationBaseBeginTime.toString(), formatterDateTime);
        //枢纽站停留时间
        Integer stationBaseMoveTime = (Integer) stationBaseRow.getAs("move_time");
        // 在00:00-00:35之间并且停留时间大于等于3分钟，并且枢纽站前有停留点 或者昨天有停留点
        if (beginTimeLocalDateTime.isBefore(localDateTime2) &&
                beginTimeLocalDateTime.isAfter(localDateTime1) &&
                stationBaseMoveTime >= THREE_MI && (hasPreStayPoint ||
                yesterdayHasStayPoint)) {
            if (!hasBehindStayPoint) { //枢纽站后无停留点
                personClassic = PersonClassic.Leave.getIndex();
            } else {//枢纽站后有停留点
                //枢纽站后第一个停留点的逗留时间
                int moveTime = (Integer) behindStayPointRow.getAs("move_time");
                DateTime beginTime = new DateTime(behindStayPointRow.getAs
                        ("begin_time"));
                DateTime endTime = new DateTime(behindStayPointRow.getAs
                        ("last_time"));
                //枢纽站后有停留点且第一个停留点B满足 move_time >= 2hr && last_time –
                // start_time < 5min或者B满足 move_time >=
                // 2hr，且B与该MSID的就业地或居住地>800m的条件（如果无职住信息，则判定为城市途经人口），则判断该用户为铁路出发人口（当天往返，但非火车返回）
                if ((Math.abs(Seconds.secondsBetween(beginTime, endTime)
                        .getSeconds()) < FIVE_MI && moveTime >= TWO_HOUR) ||
                        (moveTime >= TWO_HOUR &&
                                staypointWorkLiveDistanceGt800m(workLiveDf,
                                        msisdn, behindStayPointRow))) {
                    personClassic = PersonClassic.Leave1.getIndex();
                } else { //不满足上述条件，为城市途经人口
                    personClassic = PersonClassic.CityPassBy.getIndex();
                }
            }

        } else if (beginTimeLocalDateTime.isBefore(localDateTime4) &&
                beginTimeLocalDateTime.isAfter(localDateTime3))
        {//在22:30到23:59分之间
            if (stationBaseMoveTime >= SEVENTY_SIX && (hasBehindStayPoint ||
                    tomorrowHasStayPoint)) {//停留时间大于等于76秒且枢纽站后有停留点或该用户在后一天有停留点
                personClassic = PersonClassic.Arrive.getIndex();
            } else {
                personClassic = PersonClassic.CityPassBy.getIndex();
            }
        } else if (beginTimeLocalDateTime.isBefore(localDateTime3) &&
                beginTimeLocalDateTime.isAfter(localDateTime2)) {
            //在0:35到22:35之间

            if (hasPreStayPoint && !hasBehindStayPoint) {//枢纽站前有停留点而枢纽站后无停留点
                if (stationBaseMoveTime > THREE_MI) {
                    personClassic = PersonClassic.Leave.getIndex();
                } else {
                    personClassic = PersonClassic.NotTransportationHubLeave
                            .getIndex();
                }
            } else if (hasPreStayPoint && hasBehindStayPoint) {
                //枢纽站前有停留点且枢纽站后也有停留点
                //枢纽站后第一个停留点的逗留时间
                int moveTime = (Integer)behindStayPointRow.getAs("move_time");
                DateTime beginTime = new DateTime(behindStayPointRow.getAs
                        ("begin_time"));
                DateTime endTime = new DateTime(behindStayPointRow.getAs
                        ("last_time"));
                if (stationBaseMoveTime > THREE_MI) {
                    if ((moveTime >= TWO_HOUR && Math.abs(Seconds
                            .secondsBetween(beginTime, endTime).getSeconds())
                            < FIVE_MI) || (moveTime >= TWO_HOUR &&
                            staypointWorkLiveDistanceGt800m(workLiveDf,
                                    msisdn, behindStayPointRow))) {
                        personClassic = PersonClassic.Leave1.getIndex();
                    } else {
                        personClassic = PersonClassic.CityPassBy.getIndex();
                    }
                } else {
                    personClassic = PersonClassic.CityPassBy.getIndex();
                }
            } else if (!hasPreStayPoint && hasBehindStayPoint)
            {//枢纽站前无停留点而枢纽站后有停留点
                if (stationBaseMoveTime >= SEVENTY_SIX) {
                    personClassic = PersonClassic.Arrive.getIndex();
                } else {//若枢纽站逗留时间 < 76s，判断该用户为城市途经人口
                    personClassic = PersonClassic.CityPassBy.getIndex();
                }
            } else {//若枢纽站前无停留点且枢纽站后也无停留点，则判断该用户为城市途经人口
                personClassic = PersonClassic.CityPassBy.getIndex();
            }
        }
        //根据人口分类，创建带人口分类的信令数据集合
        for (Row row : rows) {
            Row rowWithPersionClassic = new GenericRowWithSchema(new
                    Object[]{row.getAs("date"), row.getAs("msisdn"), row
                    .getAs("base"), row.getAs("lng"), row.getAs("lat"), row
                    .getAs("begin_time"), row.getAs("last_time"), row.getAs
                    ("distance"), row.getAs("move_time"), row.getAs("speed"),
                    row.getAs("point_type"), personClassic.byteValue()},
                    ODSchemaProvider.STATION_TRACE_CLASSIC_SCHEMA);
            resultList.add(rowWithPersionClassic);
        }
        return resultList;
    }

    /**
     * 判定当前用户昨天是否有停留点
     *
     * @param stationTrace 枢纽站所有有效轨迹
     * @param yesterday    昨天
     * @param msisdn       手机号码
     * @return 用户昨天是否有停留点
     */
    private Boolean yesterdayHasStayPoint(DataFrame stationTrace, Integer
            yesterday, String msisdn) {
        DataFrame yesterdayDf = stationTrace.filter(col("date").equalTo
                (yesterday)).filter(col("msisdn").equalTo(msisdn)).filter(col
                ("point_type").equalTo(SignalProcessUtil.STAY_POINT));
        return yesterdayDf.count() > 0;
    }

    /**
     * 判定当前用户明天是否有停留点
     *
     * @param stationTrace 枢纽站所有有效轨迹
     * @param tomorrow     明天
     * @param msisdn       手机号码
     * @return 用户明天是否有停留点
     */
    private Boolean tomorrowHasStayPoint(DataFrame stationTrace, Integer
            tomorrow, String msisdn) {
        DataFrame tomorrowDayDf = stationTrace.filter(col("date").equalTo
                (tomorrow)).filter(col("msisdn").equalTo(msisdn)).filter(col
                ("point_type").equalTo(SignalProcessUtil.STAY_POINT));
        return tomorrowDayDf.count() > 0;
    }

    private Boolean staypointWorkLiveDistanceGt800m(DataFrame workLiveDf,
                                                    String msisdn, Row
                                                            staypointRow) {
        //找到用户职住信息记录
        DataFrame userWorkLiveDf = workLiveDf.filter(col("msisdn").equalTo
                (msisdn));
        Row userWorkLiveRow = userWorkLiveDf.collectAsList().get(0);
        //找到工作基站和居住基站
        String workBase = (String) userWorkLiveRow.getAs("work_base");
        String liveBase = (String) userWorkLiveRow.getAs("live_base");
        int distance = 0;
        //判定用户是否有工作基站和居住基站，如果有任何一个，计算停留点和其距离
        if (StringUtils.isEmpty(workBase) && StringUtils.isEmpty(liveBase)) {
            //如果无工作基站和居住基站，返回false
            return false;
        } else if (!StringUtils.isEmpty(workBase)) {
            distance = MapUtil.getDistance((Double) staypointRow.getAs("lng")
                    , (Double) staypointRow.getAs("lat"), (Double)
                            userWorkLiveRow.getAs("work_lng"), (Double)
                            userWorkLiveRow.getAs("work_lat"));
        } else if (!StringUtils.isEmpty(liveBase)) {
            distance = MapUtil.getDistance((Double) staypointRow.getAs("lng")
                    , (Double) staypointRow.getAs("lat"), (Double)
                            userWorkLiveRow.getAs("live_lng"), (Double)
                            userWorkLiveRow.getAs("live_lat"));
        }
        //距离是否大于800米
        return distance > 800;
    }

    /**
     * 当双枢纽基站的时候，第一个基站人口类型为 铁路出发人口（当天往返，但非火车返回），需要通过
     * 第二个基站的人口类型重新判定,当前方法返回的是最终人口分类结果，并不是第二个基站人口分类结果
     * @param rows 当天当前用户的轨迹数据
     * @param secondStationIndex 第二个基站在当前轨迹中的位置
     * @param stationTrace 枢纽站当前分析阶段所有用户数据，用于查找当前用户昨天和明天是否有停留点
     * @return 当前用户第二个枢纽站的人口分类
     */
    public Byte getPersonClassicWhenX1EqualsLeave1(List<Row> rows, int
            secondStationIndex, DataFrame stationTrace) {
        Row secondStationBase = rows.get(secondStationIndex);

        //当天日期
        Integer date = (Integer) secondStationBase.getAs("date");

        //今天的日期
        LocalDate today = LocalDate.parse(date.toString(), formatterDate);
        //明天的日期
        Integer tomorrow = Integer.valueOf(today.plusDays(1).toString
                ("yyyyMMdd"));
        //手机号码
        String msisdn = (String) secondStationBase.getAs("msisdn");
        Boolean tomorrowHasStayPoint = this.tomorrowHasStayPoint
                (stationTrace, tomorrow, msisdn);

        //定义人口分类
        Byte personClassic;
        //根据枢纽站开始数据在列车时刻表的不同位置，进行人口分类
        //列车时刻表
        LocalDateTime localDateTime1 = LocalDateTime.parse(date.toString() +
                "0000", formatterDateTimeMi);
        LocalDateTime localDateTime2 = LocalDateTime.parse(date.toString() +
                "0035", formatterDateTimeMi);
        LocalDateTime localDateTime3 = LocalDateTime.parse(date.toString() +
                "2230", formatterDateTimeMi);
        LocalDateTime localDateTime4 = LocalDateTime.parse(date.toString() +
                "2359", formatterDateTimeMi);
        //枢纽站开始时间
        Timestamp stationBaseBeginTime = (Timestamp) secondStationBase.getAs("begin_time");
        LocalDateTime beginTimeLocalDateTime = LocalDateTime.parse
                (stationBaseBeginTime.toString(), formatterDateTime);
        //枢纽站停留时间
        Integer stationBaseMoveTime = (Integer) secondStationBase.getAs("move_time");

        //第二个枢纽基站后是否有停留点
        Boolean afterSecondStationHasStayPoint = false;
        for (int i = secondStationIndex + 1; i < rows.size(); i++) {
            Byte pointType = (Byte) rows.get(i).getAs("point_type");
            if (pointType == SignalProcessUtil.STAY_POINT) {
                afterSecondStationHasStayPoint = true;
                break;
            }
        }

        /** ①若S2的start_time在（00:35 – 22:30）之间
         如果S2后有停留点，且 S2枢纽站逗留时间 >= 76s，则为往返人口，否则为城市途经人口
         **/
        if (beginTimeLocalDateTime.isBefore(localDateTime3) &&
                beginTimeLocalDateTime.isAfter(localDateTime1)) {
            if (afterSecondStationHasStayPoint && stationBaseMoveTime >=
                    SEVENTY_SIX) {
                personClassic = PersonClassic.Leave1.getIndex().byteValue();
            } else {
                personClassic = PersonClassic.CityPassBy.getIndex().byteValue();
            }
        } else {
            /**②若S2的start_time在（22:30 – 24：00）之间
             * 如果S2枢纽站逗留时间 >= 76s，S2后有停留点或该用户在后一天有停留点，则判断该用户为往返人口；否则为城市途经人口。 **/
            if (stationBaseMoveTime >= SEVENTY_SIX &&
                    (afterSecondStationHasStayPoint || tomorrowHasStayPoint)) {
                personClassic = PersonClassic.Leave1.getIndex().byteValue();
            } else {
                personClassic = PersonClassic.CityPassBy.getIndex().byteValue();
            }
        }
        return personClassic;
    }


}
