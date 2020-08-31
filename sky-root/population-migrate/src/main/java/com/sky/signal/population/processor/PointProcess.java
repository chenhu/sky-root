package com.sky.signal.population.processor;

import com.google.common.collect.Ordering;
import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.util.FileUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * @Description 区县OD分析的输入为停留点，通过合并停留点的方式组成区县od
 * @Author chenhu
 * @Date 2020/8/31 14:42
 **/
@Component
public class PointProcess implements Serializable {
    //在单个区域逗留时间阀值为60分钟
    private static final int DISTRICT_STAY_MINUTE = 1 * 60 * 60;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ParamProperties params;

    public DataFrame loadPoint(String path) {
        return FileUtil.readFile(FileUtil.FileType.PARQUET, ODSchemaProvider.TRACE_SCHEMA, path);
    }

    public DataFrame mergePointWithCell(final Broadcast<Map<String, Row>> provinceCell, DataFrame pointDf) {
        JavaRDD<Row> pointRdd = pointDf.toJavaRDD().map(new Function<Row, Row>() {
            Map<String, Row> cellMap = provinceCell.value();

            @Override
            public Row call(Row row) throws Exception {
                String base = row.getAs("base").toString();
                Integer districtCode = 0, cityCode = 0;
                try {
                    districtCode = Integer.valueOf(cellMap.get(base).getAs("district_code").toString());
                    cityCode = Integer.valueOf(cellMap.get(base).getAs("city_code").toString());
                } catch (Exception ex) {
                    districtCode = 0;
                    cityCode = 0;
                }
                return new GenericRowWithSchema(new Object[]{row.getAs("date"),
                        row.getAs("msisdn"),
                        row.getAs("base"),
                        row.getAs("lng"),
                        row.getAs("lat"),
                        row.getAs("begin_time"),
                        row.getAs("last_time"),
                        cityCode,
                        districtCode
                }, ODSchemaProvider.POINT_SCHEMA);
            }
        });
        return sqlContext.createDataFrame(pointRdd, ODSchemaProvider.POINT_SCHEMA).filter(col("district_code").notEqual(0));

    }

    /**
     * 合并区县内的停留点，并计算停留点的逗留时间
     *
     * @param rows
     * @return 同一个区县内只会有一个停留点，结果应该为 a,b,c,d这样的
     */
    private List<Row> mergePoint(List<Row> rows) {
        //结果列表
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
                int preDistrict = (Integer) pre.getAs("district_code");
                int currentDistrict = (Integer) current.getAs("district_code");
                if (preDistrict == currentDistrict) { //相同区县内的点，进行合并,增加逗留时长,合并的点不加入结果列表，等待后于遇到不同区县的点的时候再加入,如果为最后一条，加入结果列表
                    int preDurationO, durationO;
                    try {
                        preDurationO = (Integer) pre.getAs("duration_o");
                    } catch (Exception e) {
                        preDurationO = 0;
                    }
                    if (preDurationO != 0) {
                        durationO = preDurationO + Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("begin_time")), new DateTime(current.getAs("last_time"))).getSeconds());
                    } else {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(pre.getAs("begin_time")), new DateTime(pre.getAs("last_time"))).getSeconds())
                                + Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("begin_time")), new DateTime(current.getAs("last_time"))).getSeconds());
                    }
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("begin_time"),
                            current.getAs("last_time"),
                            pre.getAs("city_code"),
                            pre.getAs("district_code"),
                            durationO}, ODSchemaProvider.POINT_SCHEMA1);
                    pre = mergedRow;
                    if (loops == rows.size()) {
                        result.add(mergedRow);
                    }
                } else { //不同区县的点，把pre加入结果列表,如果为最后一条记录，把current也加入结果列表
                    int preDurationO, durationO;
                    try {
                        preDurationO = (Integer) pre.getAs("duration_o");
                    } catch (Exception e) {
                        preDurationO = 0;
                    }
                    if (preDurationO != 0) { //如果pre已经被合并过，直接加入
                        result.add(pre);
                    } else { //没有合并过，需要计算逗留时间后再加入
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(pre.getAs("begin_time")), new DateTime(pre.getAs("last_time"))).getSeconds());
                        Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                                pre.getAs("msisdn"),
                                pre.getAs("begin_time"),
                                pre.getAs("last_time"),
                                pre.getAs("city_code"),
                                pre.getAs("district_code"),
                                durationO}, ODSchemaProvider.POINT_SCHEMA1);
                        result.add(mergedRow);
                    }
                    if (loops == rows.size()) {
                        if (preDurationO != 0) {//current并不是当前区县唯一停留点
                            Row mergedRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                    current.getAs("msisdn"),
                                    current.getAs("begin_time"),
                                    current.getAs("last_time"),
                                    current.getAs("city_code"),
                                    current.getAs("district_code"),
                                    Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("begin_time")), new DateTime(current.getAs("last_time"))).getSeconds())}, ODSchemaProvider.POINT_SCHEMA1);
                            result.add(mergedRow);
                        } else { //current为当前区县唯一停留点，如果其到达时间为当天22:30后，则设置其逗留时间为1小时,否则用自身逗留时间
                            DateTime dt = new DateTime(current.getAs("begin_time"));
                            Integer hour = dt.getHourOfDay();
                            Integer minute = dt.getMinuteOfHour();
                            if (hour >= 22 && minute >= 30) {
                                Row mergedRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                        current.getAs("msisdn"),
                                        current.getAs("begin_time"),
                                        current.getAs("last_time"),
                                        current.getAs("city_code"),
                                        current.getAs("district_code"),
                                        DISTRICT_STAY_MINUTE}, ODSchemaProvider.POINT_SCHEMA1);
                                result.add(mergedRow);
                            } else {
                                Row mergedRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                        current.getAs("msisdn"),
                                        current.getAs("begin_time"),
                                        current.getAs("last_time"),
                                        current.getAs("city_code"),
                                        current.getAs("district_code"),
                                        Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("begin_time")), new DateTime(current.getAs("last_time"))).getSeconds())}, ODSchemaProvider.POINT_SCHEMA1);
                                result.add(mergedRow);
                            }
                        }
                    }
                    pre = current;
                }
                continue;
            }
        }
        return result;
    }

    /**
     * 停留点组成无逗留时间限制的区县od
     *
     * @param rows
     * @return
     */
    private List<Row> createNoneLimitedDistrictOd(List<Row> rows) {
        //无逗留时间限制的od结果列表
        List<Row> noneLimitedOd = new ArrayList<>();
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
        for (Row row : rows) {
            current = row;
            if(pre == null) {
                pre = current;
                continue;
            } else {
                Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(pre.getAs("last_time")),
                        new DateTime(current.getAs("begin_time"))).getSeconds());
                Row odRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                        pre.getAs("msisdn"),
                        pre.getAs("city_code"),
                        pre.getAs("district_code"),
                        current.getAs("city_code"),
                        current.getAs("district_code"),
                        pre.getAs("last_time"),
                        current.getAs("begin_time"),
                        pre.getAs("duration_o"),
                        current.getAs("duration_o"),
                        moveTime}, ODSchemaProvider.DISTRICT_OD_SCHEMA);
                noneLimitedOd.add(odRow);
                pre = current;
                continue;
            }
        }
        return noneLimitedOd;
    }

    /**
     * 停留点组成有时间限制的区县od
     *
     * @param rows
     * @return
     */
    private List<Row> createLimitedDistrictOd(List<Row> rows) {
        //有逗留时间限制的od结果列表
        List<Row> limitedOd = new ArrayList<>();
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
        /**生成有逗留时间限制的区县出行OD**/
        for (Row row : rows) {
            current = row;
            if(pre == null) {
                pre = current;
                continue;
            } else {
                Integer preDurationO = (Integer) pre.getAs("duration_o");
                Integer currentDurationO = (Integer) current.getAs("duration_o");
                if (preDurationO >= DISTRICT_STAY_MINUTE && currentDurationO >= DISTRICT_STAY_MINUTE) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(pre.getAs("last_time")),
                            new DateTime(current.getAs("begin_time"))).getSeconds());
                    Row odRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("city_code"),
                            pre.getAs("district_code"),
                            current.getAs("city_code"),
                            current.getAs("district_code"),
                            pre.getAs("last_time"),
                            current.getAs("begin_time"),
                            pre.getAs("duration_o"),
                            current.getAs("duration_o"),
                            moveTime}, ODSchemaProvider.DISTRICT_OD_SCHEMA);
                    limitedOd.add(odRow);
                    pre = current;
                } else if (preDurationO >= DISTRICT_STAY_MINUTE) { //pre不变
                    continue;
                } else if (currentDurationO >= DISTRICT_STAY_MINUTE) { // pre指向current
                    pre = current;
                }
            }
        }
        return limitedOd;
    }

    private JavaPairRDD<String, List<Row>> signalToJavaPairRDD(DataFrame df, ParamProperties params) {
        JavaPairRDD<String, Row> rdd = df.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                String msisdn = (String) row.getAs("msisdn");
                return new Tuple2<>(msisdn, row);
            }
        });
        //对数据进行分组，相同手机号放到一个集合里面
        List<Row> rows = new ArrayList<>();
        return rdd.aggregateByKey(rows, params.getPartitions(), new Function2<List<Row>, Row, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows1, Row row) throws Exception {
                rows1.add(row);
                return rows1;
            }
        }, new Function2<List<Row>, List<Row>, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows1, List<Row> rows2) throws Exception {
                rows1.addAll(rows2);
                return rows1;
            }
        });
    }

    public DataFrame provinceNoneLimitedOd(DataFrame odDf) {
        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                rows = mergePoint(rows);
                return createNoneLimitedDistrictOd(rows);
            }
        });
        return sqlContext.createDataFrame(odRDD, ODSchemaProvider.DISTRICT_OD_SCHEMA);
    }

    public DataFrame provinceLimitedOd(DataFrame odDf) {
        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                rows = mergePoint(rows);
                return createLimitedDistrictOd(rows);
            }
        });
        return sqlContext.createDataFrame(odRDD, ODSchemaProvider.DISTRICT_OD_SCHEMA);
    }

}
