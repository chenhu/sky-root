package com.sky.signal.pre.processor.transportationhub;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.util.SignalProcessUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chenhu on 2020/6/1.
 * 枢纽站人口分类工具类
 * 因为分类逻辑比较复杂，所以单独抽出逻辑成工具类
 */
@Slf4j
@Component
public class StationPersonClassifyUtil implements Serializable {
    private static final Integer THREE_MI = 3 * 60;
    private static final Integer FIVE_MI = 5 * 60;
    private static final Integer TWO_HOUR = 2 * 60 * 60;
    private static final Integer SEVENTY_SIX = 76;

    @Autowired
    private transient ParamProperties params;

    /**
     * 用户一天的枢纽站信令数据中只有一个枢纽基站的人口分类处理
     * <pre>
     *     4)	针对当天轨迹集合Qi = {Q1,Q2,…,Qn}中只有一个有效枢纽基站点S1 = Qj的情况，定义枢纽站前为{Q1,Q2,
     *     …,Qj-1}，枢纽站后为{Qj+1,Qj+2,…,Qn}：
     * 若枢纽站前有停留点而枢纽站后无停留点，若枢纽站逗留时间 > 3min，判断该用户为铁路出发人口；若枢纽站逗留时间 <=
     * 3min，判断该用户为非高铁出行对外交通人口；
     * 若枢纽站前有停留点且枢纽站后也有停留点，若枢纽站逗留时间 > 3min且枢纽站后的第一个停留点满足 move_time >= 2hr &&
     * last_time – start_time < 5min
     * 的条件，则判断该用户为铁路出发人口（当天往返，但非火车返回），若不满足上述条件或者枢纽站逗留时间 <= 3min，则判断该用户为城市过境人口；
     * 若枢纽站前无停留点而枢纽站后有停留点，若枢纽站逗留时间 >= 76s，判断该用户为铁路到达人口；若枢纽站逗留时间 <
     * 76s，判断该用户为城市过境人口；
     * 若枢纽站前无停留点且枢纽站后也无停留点，则判断该用户为城市过境人口。
     * </pre>
     *
     * @param rows 用户一天的枢纽站信令数据
     * @return 增加了人口分类的枢纽站信令数据
     */
    public List<Row> oneStationBaseProc(List<Row> rows) {

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

        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());
        //枢纽站的行
        Row stationBaseRow = null;
        //枢纽站行的索引位置
        int stationBaseIndex = 0;
        //定义人口分类
        Integer personClassic;
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

        for (Row row : preStationBaseList) {
            Byte stayPoint = row.getAs("stay_point");
            if (stayPoint.byteValue() == SignalProcessUtil.STAY_POINT) {
                hasPreStayPoint = true;
                break;
            }
        }
        for (Row row : behindStationBaseList) {
            Byte stayPoint = row.getAs("stay_point");
            if (stayPoint.byteValue() == SignalProcessUtil.STAY_POINT) {
                hasBehindStayPoint = true;
                behindStayPointRow = row;
                break;
            }
        }

        //枢纽站停留时间
        Integer stationBaseMoveTime = stationBaseRow.getAs("move_time");

        if (hasPreStayPoint && !hasBehindStayPoint) {//枢纽站前有停留点、后无停留点
            if (stationBaseMoveTime > THREE_MI) { //停留时间大于3分钟
                personClassic = PersonClassic.Leave.getIndex();
            } else { //停留时间小于等于3分钟
                personClassic = PersonClassic.NotTransportationHubLeave
                        .getIndex();
            }
        } else if (hasPreStayPoint && hasBehindStayPoint) {//枢纽站前后都有停留点

            if (stationBaseMoveTime > THREE_MI) { //停留时间大于3分钟
                //枢纽站后第一个停留点的逗留时间
                int moveTime = behindStayPointRow.getAs("move_time");
                DateTime beginTime = new DateTime(behindStayPointRow.getAs
                        ("begin_time"));
                DateTime endTime = new DateTime(behindStayPointRow.getAs
                        ("last_time"));
                if (Math.abs(Seconds.secondsBetween(beginTime, endTime)
                        .getSeconds()) < FIVE_MI && moveTime > TWO_HOUR) {
                    personClassic = PersonClassic.Leave1.getIndex();
                } else { //不满足上述条件，为城市过境人口
                    personClassic = PersonClassic.CityPassBy.getIndex();
                }
            } else { //停留时间小于等于3分钟
                personClassic = PersonClassic.CityPassBy.getIndex();
            }
        } else if (!hasPreStayPoint && hasBehindStayPoint) { //枢纽站前无停留点，枢纽站后有停留点
            if (stationBaseMoveTime >= SEVENTY_SIX) { //枢纽站停留时间大于等于76秒
                //城市达到人口
                personClassic = PersonClassic.Arrive.getIndex();
            } else {//城市过境人口
                personClassic = PersonClassic.CityPassBy.getIndex();
            }

        } else {//枢纽站前和站后都无停留点，城市过境人口
            personClassic = PersonClassic.CityPassBy.getIndex();
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
     * 用户一天的枢纽站信令数据中只有两个枢纽基站的人口分类处理
     * <pre>
     *     5)	针对当天轨迹集合Qi = {Q1,Q2,…,Qn}中有两个有效枢纽基站点S1,
     *     S2的情况，按照时间先后顺序，针对两个枢纽站S1,
     *     S2，以当天第一条记录到S1前一条为枢纽站前，S1后一条记录到S2前一条记录为枢纽站后，进入步骤4）计算枢纽点S1的人口类型X1
     *     ；以S1后一条记录到S2前一条记录为枢纽站前，S2后一条记录到当天最后一条记录为枢纽站后，进入步骤4）计算枢纽点S2的人口类型X2：
     * #计算两个有效枢纽基站的时间间隔T = S2（start_time）- S1（last_time）
     * #若X1=铁路出发人口（当天往返，但非火车返回），计算S1后第一个停留点B与S2的时间间隔K = S2（start_time）-
     * B（last_time）
     *
     * </pre>
     *
     * @param rows 用户一天的枢纽站信令数据
     * @return 增加了人口分类的枢纽站信令数据
     */
    public List<Row> twoStationBaseProc(List<Row> rows) {

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
        //找到两个枢纽基站的索引位置
        Integer firstStationBaseIndex = 0, secondStationBaseIndex = 0;
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).getAs("base").equals(params.getVisualStationBase
                    ())) {
                firstStationBaseIndex = i;
                break;
            }
        }
        for (int i = firstStationBaseIndex + 1; i < rows.size(); i++) {
            if (rows.get(i).getAs("base").equals(params.getVisualStationBase
                    ())) {
                secondStationBaseIndex = i;
                break;
            }
        }

        //第一个枢纽基站的前后轨迹
        List<Row> firstStationBaseTrace = rows.subList(0,
                secondStationBaseIndex);
        //第二个枢纽基站的前后轨迹
        List<Row> secondStationBaseTrace = rows.subList(firstStationBaseIndex
                + 1, rows.size());
        //分别用"只有一个枢纽站"的方法，获取人口分类
        List<Row> firstStationBaseClassification = oneStationBaseProc
                (firstStationBaseTrace);
        List<Row> secondStationBaseClassification = oneStationBaseProc
                (secondStationBaseTrace);


        return null;
    }

    /**
     * 用户一天的枢纽站信令数据中有大于两个枢纽基站的人口分类处理
     * <pre>
     *     Y > 2，判断该用户为城市过境人口
     * </pre>
     *
     * @param rows 用户一天的枢纽站信令数据
     * @return 增加了人口分类的枢纽站信令数据
     */
    public List<Row> gt2StationBaseProc(List<Row> rows) {
        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());
        //根据人口分类，创建带人口分类的信令数据集合
        for (Row row : rows) {
            Row rowWithPersionClassic = new GenericRowWithSchema(new
                    Object[]{row.getAs("date"), row.getAs("msisdn"), row
                    .getAs("base"), row.getAs("lng"), row.getAs("lat"), row
                    .getAs("begin_time"), row.getAs("last_time"), row.getAs
                    ("distance"), row.getAs("move_time"), row.getAs("speed"),
                    row.getAs("point_type"), PersonClassic.CityPassBy
                    .getIndex().byteValue()}, ODSchemaProvider
                    .STATION_TRACE_CLASSIC_SCHEMA);
            resultList.add(rowWithPersionClassic);
        }
        return resultList;
    }
}
