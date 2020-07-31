package com.sky.signal.population.processor;

import com.google.common.collect.Ordering;
import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.config.PathConfig;
import com.sky.signal.population.util.SignalProcessUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Chenhu on 2020/7/11.
 * 轨迹数据处理
 */
@Service
public class TraceProcessor implements Serializable {
    //在单个区域逗留时间阀值为120分钟
    private static final int DISTRICT_STAY_MINUTE = 2 * 60;
    @Autowired
    private transient SQLContext sqlContext;
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd " + "HH:mm:ss.S");
    private static final DateTimeFormatter FORMATTED = DateTimeFormat.forPattern("yyyyMMdd");
    @Autowired
    private transient ParamProperties params;

    @Autowired
    private transient JavaSparkContext sparkContext;

    /**
     * 加载指定路径所有的轨迹数据
     *
     * @param tracePathList 轨迹所在路径
     * @return
     */
    public DataFrame loadTrace(List<String> tracePathList) {
        DataFrame allTraceDf = null;

        for (String tracePath : tracePathList) {
            if (allTraceDf == null) {
                allTraceDf = sqlContext.read().parquet(tracePath);
            } else {
                allTraceDf = allTraceDf.unionAll(sqlContext.read().parquet(tracePath));
            }
        }
        return allTraceDf;
    }

    /**
     * 通过区县基站信息找到属于当前区县的轨迹数据
     *
     * @param currentDistrictCode 当前区县编码
     * @param allTrace            全省轨迹
     * @return
     */
    public DataFrame filterCurrentDistrictTrace(String currentDistrictCode, DataFrame allTrace) {
        return allTrace.filter(col("district").equalTo(currentDistrictCode));
    }

    /**
     * 过滤停留时间超过一定时间的手机号码的信令
     *
     * @param traceDf
     * @return
     */
    public DataFrame filterStayMsisdnTrace(final DataFrame traceDf) {
        JavaPairRDD<String, List<Row>> signalRdd = SignalProcessUtil.keyPairByMsisdn(traceDf, params);
        //过滤出在当前区县逗留时间超过DISTRICT_STAY_MINUTE的手机号
        signalRdd = signalRdd.filter(new Function<Tuple2<String, List<Row>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, List<Row>> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
                    @Override
                    public Timestamp apply(Row row) {
                        return (Timestamp) row.getAs("begin_time");
                    }
                });
                //按startTime排序
                List<Row> signals = ordering.sortedCopy(rows._2);
                if (signals.size() > 1) {
                    Timestamp beginTime = (Timestamp) signals.get(0).getAs("begin_time");
                    Timestamp endTime = (Timestamp) signals.get(signals.size() - 1).getAs("last_time");

                    int timeDiffSeconds = SignalProcessUtil.getTimeDiff(beginTime, endTime);
                    return timeDiffSeconds >= DISTRICT_STAY_MINUTE * 60;

                } else {
                    return false;
                }
            }
        });

        //保存符合逗留时间的手机号
        JavaRDD<Row> resultRdd = signalRdd.values().map(new Function<List<Row>, Row>() {
            @Override
            public Row call(List<Row> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
                    @Override
                    public Timestamp apply(Row row) {
                        return row.getAs("begin_time");
                    }
                });
                //按startTime排序
                List<Row> signals = ordering.sortedCopy(rows);
                Row row = signals.get(0);
                Timestamp beginTime = row.getAs("begin_time");
                Timestamp endTime = signals.get(signals.size() - 1).getAs("last_time");

                int moveTime = SignalProcessUtil.getTimeDiff(beginTime, endTime);

                return new GenericRowWithSchema(new Object[]{row.getAs("date"), row.getAs("msisdn"), row.getAs("region"), row.getAs("city_code"), row.getAs(
                        "district"), beginTime, endTime, moveTime}, SignalSchemaProvider.DISTRICT_SIGNAL);
            }
        });
        return sqlContext.createDataFrame(resultRdd, SignalSchemaProvider.DISTRICT_SIGNAL);
    }

    public DataFrame researchSignal(DataFrame traceDf) {
        JavaRDD<Row> rdd = traceDf.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String endTime = row.getAs("end_time").toString();
                String startTime = row.getAs("start_time").toString();
                Integer date = Integer.valueOf(DateTime.parse(startTime, FORMATTER).toString(FORMATTED));
                String msisdn = row.getAs("msisdn").toString();
                Integer region = Integer.valueOf(row.getAs("reg_city").toString());
                Integer tac = Integer.valueOf(row.getAs("lac").toString());
                Long cell = Long.valueOf(row.getAs("start_ci").toString());
                String base = null;
                double lng = 0d;
                double lat = 0d;
                String district = null;
                Integer city_code = 0;
                Timestamp begin_time = new Timestamp(DateTime.parse(startTime, FORMATTER).getMillis());
                Timestamp end_time = new Timestamp(DateTime.parse(endTime, FORMATTER).getMillis());
                Row track = RowFactory.create(date, msisdn, region, city_code, district, tac, cell, base, lng, lat, begin_time, end_time);
                return track;
            }
        });
        traceDf = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

        //过滤手机号码/基站不为0的数据,并删除重复手机信令数据
        DataFrame validDf =
                traceDf.filter(col("msisdn").notEqual("null").and(col("base").notEqual("null")).and(col("lng").notEqual(0)).and(col("lat").notEqual(0))).dropDuplicates(new String[]{"msisdn", "begin_time"});
        return validDf;

    }

    /**
     * 把基站信息合并到轨迹中
     *
     * @param traceDf
     * @param provinceCell
     * @return
     */
    public DataFrame mergeCellSignal(DataFrame traceDf, final Broadcast<Map<String, Row>> provinceCell) {
        JavaRDD<Row> rdd = traceDf.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String startTime = row.getAs("start_time").toString();
                String endTime = row.getAs("end_time").toString();
                Integer date = Integer.valueOf(DateTime.parse(startTime, FORMATTER).toString(FORMATTED));
                String msisdn = row.getAs("msisdn");
                Integer region = Integer.valueOf(row.getAs("reg_city").toString());
                Integer tac = Integer.valueOf(row.getAs("lac").toString());
                Long cell = Long.valueOf(row.getAs("start_ci").toString());
                String base = null;
                double lng = 0d;
                double lat = 0d;
                String district = null;
                Integer city_code = 0;
                Timestamp begin_time = new Timestamp(DateTime.parse(startTime, FORMATTER).getMillis());
                Timestamp end_time = new Timestamp(DateTime.parse(endTime, FORMATTER).getMillis());
                //根据tac/cell查找基站信息
                Row cellRow = provinceCell.value().get(tac.toString() + '|' + cell.toString());
                if (cellRow == null) {
                    //Do nothing
                } else {
                    city_code = cellRow.getAs("city_code");
                    base = cellRow.getAs("base");
                    lng = cellRow.getAs("lng");
                    lat = cellRow.getAs("lat");
                    district = cellRow.getAs("district");
                }

                Row track = RowFactory.create(date, msisdn, region, city_code, district, tac, cell, base, lng, lat, begin_time, end_time);
                return track;
            }
        });

        traceDf = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

        //过滤手机号码/基站不为0的数据,并删除重复手机信令数据
        DataFrame validDf =
                traceDf.filter(col("msisdn").notEqual("null").and(col("base").notEqual("null")).and(col("lng").notEqual(0)).and(col("lat").notEqual(0))).dropDuplicates(new String[]{"msisdn", "begin_time"});
        return validDf;
    }


    /**
     * 从非当前区县信令中过滤出目标区县内符合条件的手机号码的信令
     *
     * @param currentDistrictDf 目标区县信令
     * @param provinceDf        全省信令
     * @return
     */
    public DataFrame filterSignalByMsisdn(DataFrame currentDistrictDf, DataFrame provinceDf) {
        List<Row> msisdnRowList = currentDistrictDf.select("msisdn").collectAsList();
        List<String> msisdnList = new ArrayList<>(msisdnRowList.size());
        for (Row row : msisdnRowList) {
            msisdnList.add(row.getAs("msisdn").toString());
        }
        final Broadcast<List<String>> msisdnBroadcast = sparkContext.broadcast(msisdnList);

        JavaRDD<Row> signalProvinceRdd = provinceDf.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                List<String> msisdnList = msisdnBroadcast.getValue();
                String msisdn = row.getAs("msisdn");
                return msisdnList.contains(msisdn);
            }
        });
        return sqlContext.createDataFrame(signalProvinceRdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

    }

    /**
     * 按照区县生成每个人的停留点
     *
     * @param signalDf 信令数据
     * @return
     */
    public DataFrame provinceOd(DataFrame signalDf) {
        final Integer odMode = params.getOdMode();
        JavaRDD<Row> districtOdRdd = SignalProcessUtil.keyPairByMsisdn(signalDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return districtOdByMsisdn(rows,odMode);
            }
        });
        return sqlContext.createDataFrame(districtOdRdd, SignalSchemaProvider.DISTRICT_OD);

    }

    /**
     * 处理单个手机的信令，并按照日期形成手机的区县OD
     *
     * @param rows 单个用户信令
     * @return
     */
    public List<Row> districtOdByMsisdn(List<Row> rows, Integer odMode) {
        //按日期生成数据
        Map<Integer, List<Row>> signalDateMap = new HashMap<>();
        Set<Integer> dateSet = new HashSet<>();
        for (Row row : rows) {
            Integer date = row.getAs("date");
            dateSet.add(date);
        }

        //生成按照日期组成的用户轨迹
        for (Integer date : dateSet) {
            List<Row> oneDaySignalList = new ArrayList<>();
            for (Row row : rows) {
                if (row.getAs("date").equals(date)) {
                    oneDaySignalList.add(row);
                }
            }
            signalDateMap.put(date, oneDaySignalList);
        }
        List<Row> result = new ArrayList<>();
        //针对每天的用户轨迹进行处理，生成每天的区县出行OD
        for (Integer date : signalDateMap.keySet()) {
            List<Row> oneDaySignalList = signalDateMap.get(date);
            oneDaySignalList = odLink(oneDaySignalList, odMode);
            result.addAll(oneDaySignalList);
        }

        return result;
    }

    /**
     * @Description: 生成区县OD出行链
     * @Author: Hu Chen
     * @Date: 2020/7/20 17:06
     * @param: [rows]
     * @return: java.util.List<org.apache.spark.sql.Row>
     **/
    private List<Row> odLink(List<Row> rows, Integer odMode) {
        //合并单用户一条的轨迹，按照时间顺序，相邻两天记录，肯定为不同区县。
        rows = mergeDistrictTrace(rows);
        //结果列表
        List<Row> resultOd = new ArrayList<>();
        //排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("begin_time");
            }
        });
        //按startTime排序
        rows = ordering.sortedCopy(rows);
        Row pre = null;
        Row current;

        for (Row row : rows) {
            current = row;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                Integer cityCodeO = pre.getAs("city_code");
                Integer cityCodeD = current.getAs("city_code");
                String districtO = pre.getAs("district");
                String districtD = current.getAs("district");
                Timestamp beginTime = pre.getAs("begin_time");
                Timestamp endTime = current.getAs("last_time");
                int moveTime = SignalProcessUtil.getTimeDiff(beginTime, endTime);
                if (odMode == 1)//停留时间有要求
                {
                    if (moveTime >= DISTRICT_STAY_MINUTE * 60) {//停留时间满足要求
                        Row resultRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"), pre.getAs("msisdn"), pre.getAs("region"), cityCodeO,
                                districtO, cityCodeD, districtD, pre.getAs("begin_time"), current.getAs("last_time"), moveTime},
                                SignalSchemaProvider.DISTRICT_OD);
                        resultOd.add(resultRow);
                        pre = current;//pre指向current
                    } else {//停留时间不满足要求，忽略当前区县停留点，pre的指向不变
                        continue;
                    }
                } else {//停留时间无要求
                    Row resultRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"), pre.getAs("msisdn"), pre.getAs("region"), cityCodeO, districtO,
                            cityCodeD, districtD, pre.getAs("begin_time"), current.getAs("last_time"), moveTime}, SignalSchemaProvider.DISTRICT_OD);
                    resultOd.add(resultRow);
                    pre = current;
                }
            }
        }
        return resultOd;
    }

    /**
     * 合并同区县内的信令,相邻记录肯定为不同区县，同一区县只会存在一条合并后的信令，
     * 开始时间为第一条的开始时间，结束时间为最后一条的结束时间
     *
     * @param rows 单用户某天的信令
     * @return
     */
    private List<Row> mergeDistrictTrace(List<Row> rows) {
        //结果列表
        List<Row> result = new ArrayList<>();
        //排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("begin_time");
            }
        });
        //按startTime排序
        rows = ordering.sortedCopy(rows);
        Row pre = null;
        Row current;
        Map<Tuple2<String, Timestamp>, Row> tmpMap = new HashMap<>();
        for (Row row : rows) {
            current = row;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                String currentDistrict = current.getAs("district");
                String preDistrict = pre.getAs("district");
                //相同区县，合并
                if (currentDistrict.equals(preDistrict)) {
                    Timestamp beginTime = pre.getAs("begin_time");
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"), pre.getAs("msisdn"), pre.getAs("region"), pre.getAs("city_code")
                            , pre.getAs("district"), pre.getAs("tac"), pre.getAs("cell"), pre.getAs("base"), pre.getAs("lng"), pre.getAs("lat"), pre.getAs(
                                    "begin_time"), current.getAs("last_time")}, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);
                    tmpMap.put(new Tuple2<>(preDistrict, beginTime), mergedRow);
                    pre = mergedRow;
                    continue;
                } else {
                    Timestamp beginTime = current.getAs("begin_time");
                    tmpMap.put(new Tuple2<>(currentDistrict, beginTime), current);
                    pre = current;
                    continue;
                }
            }
        }

        result.addAll(tmpMap.values());

        return result;
    }


}
