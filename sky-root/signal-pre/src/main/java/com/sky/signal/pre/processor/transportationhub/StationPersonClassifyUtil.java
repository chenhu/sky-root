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
    private static final Integer TEN_MI = 10 * 60;

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
     * <pre>
     *     2020.06.11 Hu.Chen
     *     本逻辑暂时不用了，修改为一套跟当前分析枢纽站列车时刻表有关的逻辑，所以这个逻辑应该放到
     *     <code>com.sky.signal.pre.processor.transportationhub
     * .StationPersonClassify.KunShanStation</code> 中来实现
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
            if (stayPoint == SignalProcessUtil.STAY_POINT) {
                hasPreStayPoint = true;
                break;
            }
        }
        for (Row row : behindStationBaseList) {
            Byte stayPoint = row.getAs("stay_point");
            if (stayPoint == SignalProcessUtil.STAY_POINT) {
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

        //两个虚拟基站时间间隔
        DateTime beginTime = new DateTime(rows.get(secondStationBaseIndex)
                .getAs("begin_time"));
        DateTime endTime = new DateTime(rows.get(firstStationBaseIndex).getAs
                ("last_time"));

        Integer stationBaseTimeDiff = Math.abs(Seconds.secondsBetween
                (beginTime, endTime).getSeconds());
        //计算第一个虚拟基站后的第一个停留点与第二个虚拟基站之间的时间间隔

        //第一个虚拟基站后的第一个停留点的结束时间
        DateTime endTimeB = null;
        for (int i = firstStationBaseIndex; i < rows.size(); i++) {
            Byte pointType = rows.get(i).getAs("point_type");
            if (pointType == SignalProcessUtil.STAY_POINT) {
                endTimeB = rows.get(i).getAs("last_time");
                break;
            }
        }
        Integer stayPointAndSecondStationBaseTimeDiff = 0;
        if (endTimeB != null) {
            stayPointAndSecondStationBaseTimeDiff = Math.abs(Seconds
                    .secondsBetween(beginTime, endTimeB).getSeconds());
        }

        // 通过文档中的交叉表实现最终人口分类判定
        Byte x1 = firstStationBaseClassification.get(0).getAs
                ("station_person_classic");
        Byte x2 = secondStationBaseClassification.get(0).getAs
                ("station_person_classic");
        Byte personClassic = getPersonClassicByCrossTable(x1, x2,
                stationBaseTimeDiff, stayPointAndSecondStationBaseTimeDiff);

        //结果数据集
        List<Row> resultList = new ArrayList<>(rows.size());

        //根据人口分类，创建带人口分类的信令数据集合
        for (Row row : rows) {
            Row rowWithPersionClassic = new GenericRowWithSchema(new
                    Object[]{row.getAs("date"), row.getAs("msisdn"), row
                    .getAs("base"), row.getAs("lng"), row.getAs("lat"), row
                    .getAs("begin_time"), row.getAs("last_time"), row.getAs
                    ("distance"), row.getAs("move_time"), row.getAs("speed"),
                    row.getAs("point_type"), personClassic}, ODSchemaProvider
                    .STATION_TRACE_CLASSIC_SCHEMA);
            resultList.add(rowWithPersionClassic);
        }
        return resultList;
    }

    /**
     * 通过文档中的交叉表判定最终人口分类
     *
     * @param x1 第一批数据的人口分类
     * @param x2 第二批数据的人口分类
     * @param t  时间间隔1
     * @param k  时间间隔2
     * @return 最终人口分类
     */
    private Byte getPersonClassicByCrossTable(Byte x1, Byte x2, Integer t,
                                              Integer k) {
        //交叉表对角线
        if (x1.intValue() == x2.intValue()) {
            return PersonClassic.CityPassBy.getIndex().byteValue();
        }

        //x1 为城市过境人口
        if (x1.intValue() == PersonClassic.CityPassBy.getIndex() && x2
                .intValue() != PersonClassic.Arrive.getIndex()) {
            return x2;
        } else if (x1.intValue() == PersonClassic.CityPassBy.getIndex() && x2
                .intValue() == PersonClassic.Arrive.getIndex()) {
            return PersonClassic.CityPassBy.getIndex().byteValue();
        }

        //x1为铁路出发人口
        if (x1.intValue() == PersonClassic.Leave.getIndex() && x2.intValue()
                != PersonClassic.Arrive.getIndex()) {
            return PersonClassic.CityPassBy.getIndex().byteValue();
        } else if (x1.intValue() == PersonClassic.Leave.getIndex() && x2
                .intValue() == PersonClassic.Arrive.getIndex()) {
            if (t >= TWO_HOUR) {
                return PersonClassic.Leave1.getIndex().byteValue();
            } else {
                return PersonClassic.CityPassBy.getIndex().byteValue();
            }
        }

        //x1为铁路出发人口（当天往返，但非火车返回）
        if (x1.intValue() == PersonClassic.Leave1.getIndex() && x2.intValue()
                != PersonClassic.CityPassBy.getIndex()) {
            return PersonClassic.CityPassBy.getIndex().byteValue();
        } else if (x1.intValue() == PersonClassic.Leave1.getIndex() && x2
                .intValue() == PersonClassic.CityPassBy.getIndex()) {
            if (k >= TWO_HOUR) {
                return PersonClassic.Leave.getIndex().byteValue();
            } else {
                return PersonClassic.CityPassBy.getIndex().byteValue();
            }
        }

        //x1为非高铁对外交通人口
        if (x1.intValue() == PersonClassic.NotTransportationHubLeave.getIndex
                () && x2.intValue() != PersonClassic.Arrive.getIndex()) {
            return PersonClassic.CityPassBy.getIndex().byteValue();
        } else if (x1.intValue() == PersonClassic.NotTransportationHubLeave
                .getIndex() && x2.intValue() == PersonClassic.Arrive.getIndex
                ()) {
            if (t >= TWO_HOUR) {
                return PersonClassic.Arrive.getIndex().byteValue();
            } else {
                return PersonClassic.CityPassBy.getIndex().byteValue();
            }
        }

        //x1为铁路到达人口
        if (x1.intValue() == PersonClassic.Arrive.getIndex()) {
            if (x2.intValue() == PersonClassic.CityPassBy.getIndex()) {
                return PersonClassic.Arrive.getIndex().byteValue();
            }
            if (x2.intValue() == PersonClassic.Leave1.getIndex()) {
                return PersonClassic.Arrive.getIndex().byteValue();
            }

            if (x2.intValue() == PersonClassic.Leave.getIndex()) {
                if (t >= TWO_HOUR) {
                    return PersonClassic.Leave1.getIndex().byteValue();
                } else {
                    return PersonClassic.CityPassBy.getIndex().byteValue();
                }
            }

            if (x2.intValue() == PersonClassic.NotTransportationHubLeave
                    .getIndex()) {
                if (t >= TWO_HOUR) {
                    return PersonClassic.Arrive.getIndex().byteValue();
                } else {
                    return PersonClassic.CityPassBy.getIndex().byteValue();
                }
            }
        }
        //如果不在上述情况中出现，默认为城市过境人口
        return PersonClassic.CityPassBy.getIndex().byteValue();
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
    public List<Row> mergeStationBase(List<Row> rows) {
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
        //相邻的两个虚拟基站
        Row priorStationBase = null, currentStationBase = null;
        //相邻的两个虚拟基站的索引
        int priorStationBaseIndex = -1, currentStationBaseIndex = -1;

        //记录上次加入到列表中的集合index
        int checkpoint = 0;

        for (int i = 0; i < rows.size(); i++) {
            //找到虚拟基站，设置为当前虚拟基站，并记录当前虚拟基站索引
            if (rows.get(i).getAs("base").equals(params.getVisualStationBase
                    ())) {
                currentStationBase = rows.get(i);
                currentStationBaseIndex = i;
            }
            // 前虚拟基站不存在，设置为当前虚拟基站，并记录前虚拟基站的索引
            if (priorStationBase == null && currentStationBase != null) {
                priorStationBase = currentStationBase;
                priorStationBaseIndex = i;
            } else if (priorStationBase != null && currentStationBase != null
                    && !priorStationBase.equals(currentStationBase)) {//相邻虚拟基站
                DateTime beginTime = new DateTime(priorStationBase.getAs
                        ("last_time"));
                DateTime endTime = new DateTime(currentStationBase.getAs
                        ("begin_time"));
                int timeDiff = Math.abs(Seconds.secondsBetween(beginTime,
                        endTime).getSeconds());
                if (timeDiff <= TEN_MI) { //小于等于10分钟，合并两个虚拟基站

                    Row mergedStationBaseRow = SignalProcessUtil
                            .getNewRowWithStayPoint(currentStationBase, rows
                                    .get(i + 1), (Timestamp) priorStationBase
                                    .getAs("begin_time"), (Timestamp)
                                    currentStationBase.getAs("last_time"));
                    //把checkpoint开始到当前位置的记录，主要是非虚拟基站记录添加到结果列表中
                    if (checkpoint == 0) {//如果最开始的两个虚拟基站合并，需要把prior之前的记录都加入结果列表
                        resultList.addAll(rows.subList(checkpoint,
                                priorStationBaseIndex - 1));
                    }
                    //注意：合并后的虚拟基站记录先不增加进去，防止后面再出现需要舍弃或者合并的情况
                    //前基站指向合并后的基站，前基站的索引指向当前基站索引
                    priorStationBase = mergedStationBaseRow;
                    priorStationBaseIndex = currentStationBaseIndex;
                    //checkpoint 指向当前记录后
                    checkpoint = currentStationBaseIndex + 1;
                } else if (timeDiff > TEN_MI && timeDiff < TWO_HOUR) {//舍弃前虚拟基站

                    //把checkpoint开始到当前位置-1的记录，添加到结果列表中
                    //当前记录先不增加进去，防止后面再出现需要舍弃这个基站的情况
                    if (checkpoint == 0) {
                        //开始的两个虚拟基站出现舍弃情况，需要把prior之前的记录都加入结果列表
                        resultList.addAll(rows.subList(checkpoint,
                                priorStationBaseIndex - 1));
                    }
                    //把checkpoint到current之前的记录增加到结果列表
                    resultList.addAll(rows.subList(checkpoint,
                            currentStationBaseIndex - 1));

                    //chcekpoint指向current后一条
                    checkpoint = currentStationBaseIndex + 1;
                    //prior指向current
                    priorStationBase = currentStationBase;
                    priorStationBaseIndex = currentStationBaseIndex;

                } else {//两个虚拟基站都保留
                    //为了易于理解，分为三步增加
                    //1. 增加checkpoint到prior -1
                    resultList.addAll(rows.subList(checkpoint,
                            priorStationBaseIndex - 1));
                    //2. 增加prior
                    resultList.add(priorStationBase);
                    //3. 增加prior到current -1，但不增加current
                    resultList.addAll(rows.subList(priorStationBaseIndex + 1,
                            currentStationBaseIndex - 1));
                    //chcekpoint指向current后一条
                    checkpoint = currentStationBaseIndex + 1;
                    //prior指向current
                    priorStationBase = currentStationBase;
                    priorStationBaseIndex = currentStationBaseIndex;
                }
            }
            //循环进行到最后一条记录，需要把一直没有保存的current增加到结果列表
            //同时把最后一个current后的非虚拟基站记录添加到结果列表
            if (i == rows.size() - 1) {
                //因为每次都会在最后把prior指向current，所以可以增加priorStationBase
                resultList.add(priorStationBase);
                resultList.addAll(rows.subList(checkpoint, rows.size() - 1));
            }

        }
        return resultList;
    }
}
