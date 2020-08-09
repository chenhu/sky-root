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
import static org.apache.spark.sql.functions.regexp_extract;

/**
 * @Description 全省部分地市OD方面处理
 * @Author chenhu
 * @Date 2020/8/3 11:44
 **/
@Component
public class OdProcess implements Serializable {
    //在单个区域逗留时间阀值为120分钟
    private static final int DISTRICT_STAY_MINUTE = 2 * 60 * 60;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ParamProperties params;
    /**
     * @Description: 加载基础OD分析结果
     * @Author: Hu Chen
     * @Date: 2020/8/3 11:48
     * @param: []
     * @return: org.apache.spark.sql.DataFrame
     **/
    public DataFrame loadOd(String path) {
        return FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_SCHEMA, path);
    }

    /**
     * @Description: <pre>
     *      在基础OD分析结果中，增加发生地的区县编码
     *  </pre>
     * @Author: Hu Chen
     * @Date: 2020/8/3 14:21
     * @param: provinceCell, od
     * @return: 合并区县代码的OD数据
     **/
    public DataFrame mergeOdWithCell(final Broadcast<Map<String, Row>> provinceCell, DataFrame od) {
        JavaRDD<Row> odRdd = od.toJavaRDD().map(new Function<Row, Row>() {
            Map<String, Row> cellMap = provinceCell.value();

            @Override
            public Row call(Row row) throws Exception {
                String leaveBase = row.getAs("leave_base").toString();
                String arriveBase = row.getAs("arrive_base").toString();
                Integer leaveDistrictCode = 0, arriveDistrictCode = 0, leaveCityCode = 0, arriveCityCode = 0;
                try {
                    leaveDistrictCode = Integer.valueOf(cellMap.get(leaveBase).getAs("district_code").toString());
                    arriveDistrictCode = Integer.valueOf(cellMap.get(arriveBase).getAs("district_code").toString());
                    leaveCityCode = Integer.valueOf(cellMap.get(leaveBase).getAs("city_code").toString());
                    arriveCityCode = Integer.valueOf(cellMap.get(arriveBase).getAs("city_code").toString());
                } catch (Exception ex) {
                    leaveDistrictCode = 0;
                    arriveDistrictCode = 0;
                }
                return new GenericRowWithSchema(new Object[]{row.getAs("date"),
                        row.getAs("msisdn"),
                        leaveCityCode,
                        leaveDistrictCode,
                        arriveCityCode,
                        arriveDistrictCode,
                        row.getAs("leave_time"),
                        row.getAs("arrive_time"),
                        row.getAs("move_time")
                }, ODSchemaProvider.OD_DISTRICT_SCHEMA);
            }
        });
        return sqlContext.createDataFrame(odRdd, ODSchemaProvider.OD_DISTRICT_SCHEMA)
                .filter(col("leave_district").notEqual(0).and(col("arrive_district").notEqual(0)));

    }

    /**
     * 合并区县出行OD
     * @param rows 单用户一天出行OD
     * @return 区县级别的出行OD
     */
    private List<Row> mergeDistrictTrace(List<Row> rows) {
        //结果列表
        List<Row> result = new ArrayList<>();
        //排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return row.getAs("leave_time");
            }
        });
        //按出发时间排序
        rows = ordering.sortedCopy(rows);
        Row pre = null;
        Row current;
        for (Row row : rows) {
            current = row;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                int preLeaveDistrict = (Integer) pre.getAs("leave_district");
                int preArriveDistrict = (Integer) pre.getAs("arrive_district");
                int currentLeaveDistrict = (Integer) current.getAs("leave_district");
                int currentArriveDistrict = (Integer) current.getAs("arrive_district");
                //1. a-a,a-a
                if (currentLeaveDistrict == currentArriveDistrict && currentArriveDistrict == preLeaveDistrict && preLeaveDistrict == preArriveDistrict) {
                    continue;
                }
                //2. a-a,a-b
                if (preLeaveDistrict == preArriveDistrict && preArriveDistrict == currentLeaveDistrict && currentLeaveDistrict != currentArriveDistrict) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("arrive_time")), new DateTime(pre.getAs("leave_time"))).getSeconds());
                    Row mergedRow = merge1(pre, current, moveTime);
                    result.add(mergedRow);
                    pre = mergedRow;
                    continue;
                }
                //3. a-a,b-a
                if (preLeaveDistrict == preArriveDistrict && preArriveDistrict == currentArriveDistrict && currentLeaveDistrict != currentArriveDistrict) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = merge1(pre, current, moveTime);
                    pre = mergedRow;
                    continue;
                }
                //4. a-a,b-b
                if (preLeaveDistrict == preArriveDistrict && currentLeaveDistrict == currentArriveDistrict && currentLeaveDistrict != preArriveDistrict) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = merge2(pre, current, moveTime);
                    result.add(mergedRow);
                    pre = current;
                    continue;
                }
                //5. a-a, b-c
                if (preLeaveDistrict == preArriveDistrict && preArriveDistrict != currentLeaveDistrict && currentLeaveDistrict != currentArriveDistrict) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = merge2(pre, current, moveTime);
                    result.add(mergedRow);
                    moveTime = (Integer) current.getAs("move_time");
                    result.add(current);
                    pre = null;
                    continue;
                }
                //6. a-b,b-b
                if (preLeaveDistrict != preArriveDistrict && preArriveDistrict == currentLeaveDistrict && currentLeaveDistrict == currentArriveDistrict) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = merge2(pre, current, moveTime);
                    result.add(mergedRow);
                    moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("arrive_time")), new DateTime(pre.getAs
                            ("arrive_time"))).getSeconds());
                    mergedRow = merge3(pre, current, moveTime);
                    pre = mergedRow;
                    continue;
                }
                //7. a-b,a-b
                if (preLeaveDistrict == currentLeaveDistrict && preArriveDistrict == currentArriveDistrict && preLeaveDistrict != preArriveDistrict) {
                    result.add(pre);
                    result.add(current);
                    pre = null;
                    continue;
                }

                //8. a-b,b-a
                if (preLeaveDistrict == currentArriveDistrict && preArriveDistrict == currentLeaveDistrict && preLeaveDistrict != preArriveDistrict) {
                    result.add(pre);
                    result.add(current);
                    pre = null;
                    continue;
                }
                //9. a-b,b-c
                if (preArriveDistrict == currentLeaveDistrict && preLeaveDistrict != preArriveDistrict &&
                        currentLeaveDistrict != currentArriveDistrict && preLeaveDistrict != currentArriveDistrict) {
                    result.add(pre);
                    result.add(current);
                    pre = null;
                    continue;
                }
                //10. a-b,c-c
                if (preLeaveDistrict != preArriveDistrict && currentLeaveDistrict == currentArriveDistrict &&
                        preLeaveDistrict != currentLeaveDistrict && preArriveDistrict != currentLeaveDistrict) {
                    result.add(pre);
                    pre = current;
                    continue;
                }
            }
        }
        return result;
    }

    /**
     * 1. a-a, a-b
     * 2. a-a, b-a
     *
     * @param pre
     * @param current
     * @param moveTime
     * @return
     */
    private Row merge1(Row pre, Row current, Integer moveTime) {
        Integer leaveDistrict = (Integer) pre.getAs("leave_district");
        Integer leaveCity = (Integer) pre.getAs("leave_city");
        Integer arriveDistrict = (Integer) current.getAs("arrive_district");
        Integer arriveCity = (Integer) current.getAs("arrive_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                pre.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                pre.getAs("leave_time"),
                current.getAs("arrive_time"),
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA);
        return mergedRow;
    }

    /**
     * 1. a-a,b-b
     *
     * @param pre
     * @param current
     * @param moveTime
     * @return
     */
    private Row merge2(Row pre, Row current, Integer moveTime) {
        Integer leaveDistrict = (Integer) pre.getAs("leave_district");
        Integer leaveCity = (Integer) pre.getAs("leave_city");
        Integer arriveDistrict = (Integer) current.getAs("leave_district");
        Integer arriveCity = (Integer) current.getAs("leave_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                pre.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                pre.getAs("leave_time"),
                current.getAs("leave_time"),
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA);
        return mergedRow;
    }

    private Row merge3(Row pre, Row current, Integer moveTime) {
        Integer leaveDistrict = (Integer) pre.getAs("leave_district");
        Integer leaveCity = (Integer) pre.getAs("leave_city");
        Integer arriveDistrict = (Integer) current.getAs("leave_district");
        Integer arriveCity = (Integer) current.getAs("leave_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                current.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                pre.getAs("arrive_time"),
                current.getAs("arrive_time"),
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA);
        return mergedRow;
    }

    /**
     * 有停留时间要求的情况：给区县出行OD增加O点逗留时长和D点逗留时长，并重新组成符合条件的出行OD
     * @param rows
     * @return
     */
    private List<Row> addDurationForOD1(List<Row> rows) {
        return null;
    }

    /**
     * 没有有停留时间要求的情况：给区县出行OD增加O点逗留时长和D点逗留时长
     * @param rows
     * @return
     */
    private List<Row> addDurationForOD2(List<Row> rows) {
        return null;
    }

    /**
     * 手机信令DataFrame转化为JavaPairRDD
     *
     * @param df
     * @param params
     * @return
     */
    private  JavaPairRDD<String, List<Row>> signalToJavaPairRDD
    (DataFrame df, ParamProperties params) {
        JavaPairRDD<String, Row> rdd = df.javaRDD()
                .mapToPair(new PairFunction<Row, String, Row>() {
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        String msisdn = row.getAs("msisdn");
                        return new Tuple2<>(msisdn, row);
                    }
                });

        //对数据进行分组，相同手机号放到一个集合里面
        List<Row> rows = new ArrayList<>();
        return rdd.aggregateByKey(rows, params.getPartitions(),
                new Function2<List<Row>, Row, List<Row>>() {
                    @Override
                    public List<Row> call(List<Row> rows1, Row row) throws Exception {
                        rows1.add(row);
                        return rows1;
                    }
                }, new Function2<List<Row>, List<Row>, List<Row>>() {
                    @Override
                    public List<Row> call(List<Row> rows1, List<Row> rows2) throws
                            Exception {
                        rows1.addAll(rows2);
                        return rows1;
                    }
                });
    }

    /**
     * 获取全省OD中，无出行时间阀值要求
     *
     * @param odDf 全省OD数据
     * @return
     */
    public DataFrame provinceResultOd2(DataFrame odDf) {

        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf,params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                rows =  mergeDistrictTrace(rows);
                return addDurationForOD2(rows);
            }
        });

        return sqlContext.createDataFrame(odRDD,ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
    }

    /**
     * 获取全省OD中，有出行时间阀值要求
     *
     * @param odDf 全省OD数据
     * @return
     */
    public DataFrame provinceResultOd1(DataFrame odDf) {

        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf,params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                rows =  mergeDistrictTrace(rows);
                return addDurationForOD1(rows);
            }
        });

        odDf = sqlContext.createDataFrame(odRDD,ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
        //找出逗留时间满足要求的od
        final Integer odMode = params.getOdMode();
        if (odMode != 0) {//对逗留时间有要求，对OD进行过滤
            odDf = odDf.filter(col("move_time").geq(DISTRICT_STAY_MINUTE));
        }
        return odDf;
    }
}
