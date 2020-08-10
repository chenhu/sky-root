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
     *
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
                return (Timestamp) row.getAs("leave_time");
            }
        });
        //按出发时间排序
        rows = ordering.sortedCopy(rows);
        Row pre = null;
        Row current;

        //缺少D点逗留时间的OD
        Row unCompleteOd = null;

        int loops = 0;
        //获取当天时间的23:59:59秒
        DateTime dt = new DateTime(rows.get(0).getAs("leave_time"));
        DateTime lastDt = new DateTime(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), 23, 59, 59);

        for (Row row : rows) {
            loops ++;
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
                    Integer durationO;
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd
                                .getAs("arrive_time")), new DateTime(current.getAs
                                ("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    }else {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(pre
                                .getAs("leave_time")), new DateTime(pre.getAs("arrive_time"))).getSeconds());
                    }
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("arrive_time")), new DateTime(current.getAs("leave_time"))).getSeconds());
                    Row mergedRow = merge1(pre, current, durationO, moveTime);
                    //current为最后一条记录
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("arrive_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow);
                        result.add(mergedRow);
                    } else {
                        unCompleteOd = mergedRow;
                        pre = mergedRow;
                    }

                    continue;
                }
                //3. a-a,b-a,忽略b点，合并
                if (preLeaveDistrict == preArriveDistrict && preArriveDistrict == currentArriveDistrict && currentLeaveDistrict != currentArriveDistrict) {
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("arrive_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = merge(pre, current, moveTime);
                    pre = mergedRow;
                    continue;
                }
                //4. a-a,b-b,合并为a-b
                if (preLeaveDistrict == preArriveDistrict && currentLeaveDistrict == currentArriveDistrict && currentLeaveDistrict != preArriveDistrict) {
                    Integer durationO;
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")), new DateTime(pre.getAs
                                ("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    }else {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(pre.getAs("leave_time")),
                                        new DateTime(pre.getAs("arrive_time"))).getSeconds());
                    }
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = merge2(pre, current,durationO, moveTime);
                    //current为最后一条记录
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("leave_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow);
                        result.add(mergedRow);
                    } else {
                        unCompleteOd = mergedRow;
                        pre = mergedRow;
                    }
                    continue;
                }
                //5. a-a, b-c 合并为a-b,b-c
                if (preLeaveDistrict == preArriveDistrict && preArriveDistrict != currentLeaveDistrict && currentLeaveDistrict != currentArriveDistrict) {
                    Integer durationO;
                    //完成上一个
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")),
                                new DateTime(pre.getAs("arrive_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    } else {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(pre
                                .getAs("leave_time")), new DateTime(pre.getAs("arrive_time"))).getSeconds());
                    }
                    //a-b
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("leave_time")), new DateTime(pre.getAs("arrive_time"))).getSeconds());
                    Integer durationD =  Math.abs(Seconds.secondsBetween(new DateTime(current.getAs("leave_time")), new DateTime(pre.getAs("arrive_time"))).getSeconds());
                    Row mergedRow = merge4(pre, current, durationO, durationD,moveTime);
                    result.add(mergedRow);
                    //b-c
                    Row anotherRow = merge5(current,durationD);
                    //current为最后一条记录
                    if(loops == rows.size()) {
                        durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("leave_time")), lastDt).getSeconds());
                        completeDurationD(durationD,anotherRow);
                        result.add(anotherRow);
                    } else {
                        unCompleteOd = anotherRow;
                        pre = anotherRow;
                    }
                    continue;
                }
                //6. a-b,b-b
                if (preLeaveDistrict != preArriveDistrict && preArriveDistrict == currentLeaveDistrict && currentLeaveDistrict == currentArriveDistrict) {
                    Integer durationO;
                    //完成上一个
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")),
                                new DateTime(pre.getAs("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    } else {
                        durationO = 0;
                    }

                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(pre
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("arrive_time"))).getSeconds());
                    Row mergedRow = merge6(pre, durationO,moveTime);
                    //current为最后一条记录
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(pre
                                .getAs("arrive_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow);
                        result.add(mergedRow);
                    } else {
                        unCompleteOd = mergedRow;
                        pre = current;
                    }
                    continue;
                }
                //7. a-b,a-b
                if (preLeaveDistrict == currentLeaveDistrict && preArriveDistrict == currentArriveDistrict && preLeaveDistrict != preArriveDistrict) {
                    Integer durationO;
                    //完成上一个
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")),
                                new DateTime(pre.getAs("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    } else {
                        durationO = 0;
                    }
                    Row mergedRow = merge7(pre,durationO);
                    result.add(mergedRow);
                    Row mergedRow2 = merge7(current,0);
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("arrive_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow2);
                        result.add(mergedRow2);
                    } else {
                        unCompleteOd = mergedRow2;
                        pre = mergedRow2;
                    }
                    continue;
                }

                //8. a-b,b-a
                if (preLeaveDistrict == currentArriveDistrict && preArriveDistrict == currentLeaveDistrict && preLeaveDistrict != preArriveDistrict) {

                    Integer durationO;
                    //完成上一个
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")),
                                new DateTime(pre.getAs("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    } else {
                        durationO = 0;
                    }
                    Row mergedRow = merge7(pre,durationO);
                    result.add(mergedRow);
                    Row mergedRow2 = merge7(current,0);
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("arrive_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow2);
                        result.add(mergedRow2);
                    } else {
                        unCompleteOd = mergedRow2;
                        pre = mergedRow2;
                    }
                    continue;
                }
                //9. a-b,b-c
                if (preArriveDistrict == currentLeaveDistrict && preLeaveDistrict != preArriveDistrict &&
                        currentLeaveDistrict != currentArriveDistrict && preLeaveDistrict != currentArriveDistrict) {
                    Integer durationO;
                    //完成上一个
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")),
                                new DateTime(pre.getAs("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    } else {
                        durationO = 0;
                    }
                    Row mergedRow = merge7(pre,durationO);
                    result.add(mergedRow);
                    Row mergedRow2 = merge7(current,0);
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("arrive_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow2);
                        result.add(mergedRow2);
                    } else {
                        unCompleteOd = mergedRow2;
                        pre = mergedRow2;
                    }
                    continue;
                }
                //10. a-b,c-c 合并为 a-b, b-c
                if (preLeaveDistrict != preArriveDistrict && currentLeaveDistrict == currentArriveDistrict &&
                        preLeaveDistrict != currentLeaveDistrict && preArriveDistrict != currentLeaveDistrict) {
                    Integer durationO;
                    //完成上一个
                    if(unCompleteOd != null) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(unCompleteOd.getAs("arrive_time")),
                                new DateTime(pre.getAs("leave_time"))).getSeconds());
                        completeDurationD(durationD,unCompleteOd);
                        result.add(unCompleteOd);
                        durationO = durationD;
                    } else {
                        durationO = 0;
                    }
                    Row mergedRow = merge7(pre,durationO);
                    result.add(mergedRow);
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("leave_time")), new DateTime(pre.getAs
                            ("arrive_time"))).getSeconds());
                    Row mergedRow2 = merge8(pre,current,0,moveTime);
                    if(loops == rows.size()) {
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("leave_time")), lastDt).getSeconds());
                        completeDurationD(durationD,mergedRow2);
                        result.add(mergedRow2);
                    } else {
                        unCompleteOd = mergedRow2;
                        pre = mergedRow2;
                    }
                    continue;
                }
            }
        }
        return result;
    }

    private void completeDurationD(Integer durationD, Row unCompleteOD) {
        if(unCompleteOD != null) {
            unCompleteOD = new GenericRowWithSchema(new Object[]{unCompleteOD.getAs("date"),
                    unCompleteOD.getAs("msisdn"),
                    unCompleteOD.getAs("leave_city"),
                    unCompleteOD.getAs("leave_district"),
                    unCompleteOD.getAs("arrive_city"),
                    unCompleteOD.getAs("arrive_district"),
                    unCompleteOD.getAs("leave_time"),
                    unCompleteOD.getAs("arrive_time"),
                    unCompleteOD.getAs("duration_o"),
                    durationD,
                    unCompleteOD.getAs("move_time")}, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
        }
    }

    private Row merge(Row pre, Row current, Integer moveTime) {
        Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                pre.getAs("msisdn"),
                pre.getAs("leave_city"),
                pre.getAs("leave_district"),
                current.getAs("arrive_city"),
                current.getAs("arrive_district"),
                pre.getAs("leave_time"),
                current.getAs("arrive_time"),
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA);
        return mergedRow;
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
    private Row merge1(Row pre, Row current, Integer durationO, Integer moveTime) {
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
                durationO,
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
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
    private Row merge2(Row pre, Row current, Integer durationO,Integer moveTime) {
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
                durationO,
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
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

    private Row merge4(Row pre, Row current, Integer durationO, Integer durationD,Integer moveTime) {
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
                durationO,
                durationD,
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
        return mergedRow;
    }

    private Row merge5(Row current, Integer durationO) {
        Integer leaveDistrict = (Integer) current.getAs("leave_district");
        Integer leaveCity = (Integer) current.getAs("leave_city");
        Integer arriveDistrict = (Integer) current.getAs("arrive_district");
        Integer arriveCity = (Integer) current.getAs("arrive_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                current.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                current.getAs("leave_time"),
                current.getAs("arrive_time"),
                durationO,
                current.getAs("move_time"),}, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
        return mergedRow;
    }

    private Row merge6(Row pre, Integer durationO,Integer moveTime) {
        Integer leaveDistrict = (Integer) pre.getAs("leave_district");
        Integer leaveCity = (Integer) pre.getAs("leave_city");
        Integer arriveDistrict = (Integer) pre.getAs("arrive_district");
        Integer arriveCity = (Integer) pre.getAs("arrive_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                pre.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                pre.getAs("leave_time"),
                pre.getAs("arrive_time"),
                durationO,
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
        return mergedRow;
    }

    private Row merge7(Row row,Integer durationO) {
        Integer leaveDistrict = (Integer) row.getAs("leave_district");
        Integer leaveCity = (Integer) row.getAs("leave_city");
        Integer arriveDistrict = (Integer) row.getAs("arrive_district");
        Integer arriveCity = (Integer) row.getAs("arrive_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{row.getAs("date"),
                row.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                row.getAs("leave_time"),
                row.getAs("arrive_time"),
                durationO,
                row.getAs("move_time")}, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
        return mergedRow;
    }

    private Row merge8(Row pre,Row current, Integer durationO,Integer moveTime) {
        Integer leaveDistrict = (Integer) pre.getAs("arrive_district");
        Integer leaveCity = (Integer) pre.getAs("arrive_city");
        Integer arriveDistrict = (Integer) current.getAs("arrive_district");
        Integer arriveCity = (Integer) current.getAs("arrive_city");
        Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                pre.getAs("msisdn"),
                leaveCity,
                leaveDistrict,
                arriveCity,
                arriveDistrict,
                pre.getAs("arrive_time"),
                current.getAs("leave_time"),
                durationO,
                moveTime}, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
        return mergedRow;
    }

    /**
     * 有停留时间要求的情况：给区县出行OD增加O点逗留时长和D点逗留时长，并重新组成符合条件的出行OD
     *
     * @param rows
     * @return
     */
    private List<Row> addDurationForOD1(List<Row> rows) {
        return null;
    }

    /**
     * 没有有停留时间要求的情况：给区县出行OD增加O点逗留时长和D点逗留时长
     *
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
    private JavaPairRDD<String, List<Row>> signalToJavaPairRDD
    (DataFrame df, ParamProperties params) {
        JavaPairRDD<String, Row> rdd = df.javaRDD()
                .mapToPair(new PairFunction<Row, String, Row>() {
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        String msisdn = (String) row.getAs("msisdn");
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

        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return mergeDistrictTrace(rows);
            }
        });

        return sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
    }

    /**
     * 获取全省OD中，有出行时间阀值要求
     *
     * @param odDf 全省OD数据
     * @return
     */
    public DataFrame provinceResultOd1(DataFrame odDf) {

        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                rows = mergeDistrictTrace(rows);
                return addDurationForOD1(rows);
            }
        });

        odDf = sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
        //找出逗留时间满足要求的od
        final Integer odMode = params.getOdMode();
        if (odMode != 0) {//对逗留时间有要求，对OD进行过滤
            odDf = odDf.filter(col("move_time").geq(DISTRICT_STAY_MINUTE));
        }
        return odDf;
    }
}
