package com.sky.signal.population.processor;

import com.google.common.collect.Ordering;
import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.util.FileUtil;
import lombok.Data;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

@Component
public class OdProcess implements Serializable {
    //在单个区域逗留时间阀值为60分钟
    private static final int DISTRICT_STAY_MINUTE = 1 * 60 * 60;
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
        return FileUtil.readFile(FileUtil.FileType.PARQUET, ODSchemaProvider.OD_SCHEMA, path);
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
                        row.getAs("duration_o")
                }, ODSchemaProvider.OD_DISTRICT_SCHEMA);
            }
        });
        return sqlContext.createDataFrame(odRdd, ODSchemaProvider.OD_DISTRICT_SCHEMA)
                .filter(col("leave_district").notEqual(0).and(col("arrive_district").notEqual(0)));

    }


    /**
     * 合并连续起点和终点相同的od,同一个区县内，应该为a-a,a-b这样的od
     * 本方法需要递归处理
     * @param rows
     * @return
     */
    private List<Row> mergeSameDistrictOD(List<Row> rows) {
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
        int loops = 0;
        for (Row row : rows) {
            current = row;
            loops ++;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                int preLeaveDistrict = (Integer) pre.getAs("leave_district");
                int preArriveDistrict = (Integer) pre.getAs("arrive_district");
                int currentLeaveDistrict = (Integer) current.getAs("leave_district");
                int currentArriveDistrict = (Integer) current.getAs("arrive_district");
                //判断两条od出发点和到达点是否在一个区县
                if(preLeaveDistrict != preArriveDistrict) {//不同区县，加入结果列表，进入下次循环，pre指向current
                    result.add(pre);
                    pre = current;
                    if(loops == rows.size()) {//current为最后一条数据
                        result.add(current);
                    }
                    continue;
                } else if(currentLeaveDistrict ==currentArriveDistrict && preLeaveDistrict==currentLeaveDistrict) {//pre和current都在相同区县，合并
                    int durationO = (Integer) pre.getAs("duration_o") + (Integer) current.getAs("duration_o");
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("leave_city"),
                            pre.getAs("leave_district"),
                            pre.getAs("arrive_city"),
                            pre.getAs("arrive_district"),
                            pre.getAs("leave_time"),
                            current.getAs("arrive_time"),
                            durationO}, ODSchemaProvider.OD_DISTRICT_SCHEMA);
//                    result.add(mergedRow);
                    pre = mergedRow;
                    continue;

                } else if(currentLeaveDistrict ==currentArriveDistrict && preLeaveDistrict!=currentLeaveDistrict) {//current为异常数据,忽略current,进入下个循环,pre不变
                    if(loops == rows.size()) {//current为最后一条数据
                        result.add(current);
                    }
                    continue;
                } else if(preArriveDistrict != currentLeaveDistrict && currentLeaveDistrict !=currentArriveDistrict) {//a-a,b-c
                    result.add(pre);
                    pre = current;
                    if(loops == rows.size()) {//current为最后一条数据
                        result.add(current);
                    }
                    continue;
                } else if(preArriveDistrict == currentLeaveDistrict && currentLeaveDistrict !=currentArriveDistrict) {//a-a,a-b
                    result.add(pre);
                    pre = current;
                    if(loops == rows.size()) {//current为最后一条数据
                        result.add(current);
                    }
                    continue;
                } else {//pre指向current，进入下次循环
                    pre = current;
                    if(loops == rows.size()) {//current为最后一条数据
                        result.add(current);
                    }
                    continue;
                }
            }
        }
        return result;
    }

    /**
     * 合并a-a到a-b的a，最终应该为a-b,b-c,c-d这样的od,包含durationO，但是没有durationD
     * @param rows
     * @return
     */
    private List<Row> mergeDistrictOD(List<Row> rows) {
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

        int loops = 0;
        Row preAdd = null;
        for (Row row : rows) {
            current = row;
            loops++;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                int preLeaveDistrict = (Integer) pre.getAs("leave_district");
                int preArriveDistrict = (Integer) pre.getAs("arrive_district");
                int currentLeaveDistrict = (Integer) current.getAs("leave_district");
                int currentArriveDistrict = (Integer) current.getAs("arrive_district");
                //判断两条od出发点和到达点是否在一个区县
                if(preLeaveDistrict != preArriveDistrict) {//不同区县，加入结果列表，进入下次循环，pre指向current
                    Integer durationO ;
                    if(preAdd != null && preAdd.getAs("arrive_district").equals(preLeaveDistrict)) {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(preAdd
                                .getAs("arrive_time")), new DateTime(pre.getAs
                                ("leave_time"))).getSeconds());
                    } else {
                        durationO = (Integer) pre.getAs("duration_o");
                    }
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(pre
                            .getAs("arrive_time")), new DateTime(pre.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("leave_city"),
                            pre.getAs("leave_district"),
                            pre.getAs("arrive_city"),
                            pre.getAs("arrive_district"),
                            pre.getAs("leave_time"),
                            pre.getAs("arrive_time"),
                            durationO,moveTime
                    }, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
                    result.add(mergedRow);
                    preAdd = mergedRow;
                    pre = current;
                    if(loops == rows.size()) {//current为最后一条数据
                        if(currentLeaveDistrict !=currentArriveDistrict && preLeaveDistrict==currentLeaveDistrict) {//a-b,b-x
                            Integer lastMoveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                                    .getAs("arrive_time")), new DateTime(current.getAs
                                    ("leave_time"))).getSeconds());
                            Row lastRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                    current.getAs("msisdn"),
                                    current.getAs("leave_city"),
                                    current.getAs("leave_district"),
                                    current.getAs("arrive_city"),
                                    current.getAs("arrive_district"),
                                    current.getAs("leave_time"),
                                    current.getAs("arrive_time"),
                                    Math.abs(Seconds.secondsBetween(new DateTime(pre
                                            .getAs("arrive_time")), new DateTime(current.getAs
                                            ("leave_time"))).getSeconds()),
                                    lastMoveTime
                            }, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
                            result.add(lastRow);
                        } else { //a-b,b-b
                            Integer lastMoveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                                    .getAs("arrive_time")), new DateTime(current.getAs
                                    ("leave_time"))).getSeconds());
                            Row lastRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                    current.getAs("msisdn"),
                                    current.getAs("leave_city"),
                                    current.getAs("leave_district"),
                                    current.getAs("arrive_city"),
                                    current.getAs("arrive_district"),
                                    current.getAs("leave_time"),
                                    current.getAs("arrive_time"),
                                    current.getAs("duration_o"),
                                    lastMoveTime
                            }, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
                            result.add(lastRow);
                        }
                    }
                    continue;
                } else if(currentLeaveDistrict !=currentArriveDistrict && preLeaveDistrict==currentLeaveDistrict) {//a-a,a-b，合并
                    Integer durationO ;
                    if(preAdd != null && preAdd.getAs("arrive_district").equals(currentLeaveDistrict)) {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(preAdd
                                .getAs("arrive_time")), new DateTime(current.getAs
                                ("leave_time"))).getSeconds());
                    } else {
                        durationO = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("leave_time")), new DateTime(pre.getAs
                                ("leave_time"))).getSeconds());
                    }
                    Integer moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                            .getAs("arrive_time")), new DateTime(current.getAs
                            ("leave_time"))).getSeconds());
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("leave_city"),
                            pre.getAs("leave_district"),
                            current.getAs("arrive_city"),
                            current.getAs("arrive_district"),
                            current.getAs("leave_time"),
                            current.getAs("arrive_time"),
                            durationO,moveTime
                    }, ODSchemaProvider.OD_DISTRICT_SCHEMA_O);
                    result.add(mergedRow);
                    preAdd = mergedRow;
                    pre = null;
                    continue;

                }  else {//其他为异常数据, pre指向current，进入下次循环
                    pre = current;
                    continue;
                }

            }
        }
        return result;
    }

    /**
     * 输入为a-b,b-c,c-d这样的od，需要对这样的数据生成D点的逗留时间
     *
     * @param rows
     * @return
     */
    private List<Row> createDurationD(List<Row> rows) {
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
        DateTime dt = null, lastDt = null;
        //获取当天时间的23:59:59秒
        if(rows.size()>0) {
            dt = new DateTime(rows.get(0).getAs("leave_time"));
            lastDt = new DateTime(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), 23, 59, 59);
        }
        int loops = 0;
        for (Row row : rows) {
            current = row;
            loops ++;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                int preLeaveDistrict = (Integer) pre.getAs("leave_district");
                int preArriveDistrict = (Integer) pre.getAs("arrive_district");
                int currentLeaveDistrict = (Integer) current.getAs("leave_district");
                int currentArriveDistrict = (Integer) current.getAs("arrive_district");
                //判断两条od出发点和到达点是否在一个区县
                if(preLeaveDistrict != preArriveDistrict) {//不同区县，加入结果列表，进入下次循环，pre指向current
                    Integer durationD;
                    if(loops == rows.size()) {//current为最后一条数据
                        if(currentLeaveDistrict == currentArriveDistrict && currentLeaveDistrict == preArriveDistrict) { //a-b,b-b (上个步骤中的问题在这个步骤处理)
                            durationD = Math.abs(Seconds.secondsBetween(new DateTime(pre.getAs("arrive_time")), lastDt).getSeconds());
                        } else {
                            durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                    .getAs("leave_time")), lastDt).getSeconds());
                            Row lastRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                    current.getAs("msisdn"),
                                    current.getAs("leave_city"),
                                    current.getAs("leave_district"),
                                    current.getAs("arrive_city"),
                                    current.getAs("arrive_district"),
                                    current.getAs("leave_time"),
                                    current.getAs("arrive_time"),
                                    current.getAs("duration_o"),
                                    durationD,
                                    current.getAs("move_time")
                            }, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
                            result.add(lastRow);
                        }
                    } else {
                        durationD = (Integer) current.getAs("duration_o");
                    }
                    Row mergedRow = new GenericRowWithSchema(new Object[]{pre.getAs("date"),
                            pre.getAs("msisdn"),
                            pre.getAs("leave_city"),
                            pre.getAs("leave_district"),
                            pre.getAs("arrive_city"),
                            pre.getAs("arrive_district"),
                            pre.getAs("leave_time"),
                            pre.getAs("arrive_time"),
                            pre.getAs("duration_o"),
                            durationD,
                            pre.getAs("move_time")
                    }, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
                    result.add(mergedRow);
                    pre = current;
                    continue;
                } else {//正常情况下,不应该出现这种情况,丢弃pre,进入下次循环
                    pre = current;
                    if(loops == rows.size()) {//current为最后一条数据
                        Integer durationD = Math.abs(Seconds.secondsBetween(new DateTime(current
                                .getAs("leave_time")), lastDt).getSeconds());
                        Row lastRow = new GenericRowWithSchema(new Object[]{current.getAs("date"),
                                current.getAs("msisdn"),
                                current.getAs("leave_city"),
                                current.getAs("leave_district"),
                                current.getAs("arrive_city"),
                                current.getAs("arrive_district"),
                                current.getAs("leave_time"),
                                current.getAs("arrive_time"),
                                current.getAs("duration_o"),
                                durationD,
                                current.getAs("move_time")
                        }, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
                        result.add(lastRow);
                    }
                    continue;
                }
            }
        }
        return result;
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
    public DataFrame provinceResultOd(DataFrame odDf) {
        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                int beforeSize, afterSize;
                //合并连续起点和终点相同的od,同一个区县内，应该为a-a,a-b这样的od
                do {
                    beforeSize = rows.size();
                    rows = mergeSameDistrictOD(rows);
                    afterSize = rows.size();
                } while (beforeSize - afterSize > 0);
                //合并a-a到a-b的a，最终应该为a-b,b-c,c-d这样的od
                rows = mergeDistrictOD(rows);
                //生成D点的逗留时间
                rows = createDurationD(rows);
                return rows;
            }
        });
        return   sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
    }


    /**
     * 根据已经生成的，停留时间没有时间限制的区县出行OD，生成有停留时间限制的区县出行OD
     * @param odDf 停留时间没有时间限制的区县出行OD
     * @return 有停留时间限制的区县出行OD
     */
    public DataFrame provinceDurationLimitedOd(DataFrame odDf) {
        JavaRDD<Row> odRDD = this.signalToJavaPairRDD(odDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return timeLimitedOd(rows);
            }
        });
        return   sqlContext.createDataFrame(odRDD, ODSchemaProvider.OD_DISTRICT_SCHEMA_DET);
    }

    /**
     * 合并区县内od，有逗留时间要求
     * @param rows
     * @return
     */
    private List<Row>  timeLimitedOd(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        //1. 按照日期排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
            @Override
            public Timestamp apply(Row row) {
                return (Timestamp) row.getAs("leave_time");
            }
        });
        rows = ordering.sortedCopy(rows);
        //2. 并把od拆开，变为a-b-c-d这样的，把符合时间要求的点加入到有序列表中
        List<OdPoint> points = new ArrayList<>();
        for(Row row: rows) {
            if((Integer)row.getAs("duration_o") >= DISTRICT_STAY_MINUTE ) {
                OdPoint odPoint = new OdPoint();
                odPoint.setMsisdn(row.getAs("msisdn").toString());
                odPoint.setDate((Integer)row.getAs("date"));
                odPoint.setDuration((Integer)(row.getAs("duration_o")));
                odPoint.setCity((Integer)(row.getAs("leave_city")));
                odPoint.setDistrict((Integer)(row.getAs("leave_district")));
                odPoint.setMoveTime((Integer)(row.getAs("move_time")));
                odPoint.setTime((Timestamp)(row.getAs("leave_time")));
                points.add(odPoint);
            }
            if((Integer)row.getAs("duration_d") >= DISTRICT_STAY_MINUTE ) {
                OdPoint odPoint = new OdPoint();
                odPoint.setMsisdn(row.getAs("msisdn").toString());
                odPoint.setDate((Integer)row.getAs("date"));
                odPoint.setDuration((Integer)(row.getAs("duration_d")));
                odPoint.setCity((Integer)(row.getAs("arrive_city")));
                odPoint.setDistrict((Integer)(row.getAs("arrive_district")));
                odPoint.setMoveTime((Integer)(row.getAs("move_time")));
                odPoint.setTime((Timestamp)(row.getAs("arrive_time")));
                points.add(odPoint);
            }
        }
        //3. 重新组成od
        OdPoint pre = null;
        OdPoint current;
        for (OdPoint odPoint : points) {
            current = odPoint;
            if (pre == null) {
                pre = current;
                continue;
            } else {
                if(pre.getDistrict().intValue() != current.getDistrict().intValue()) {

                    Row concatRow = new GenericRowWithSchema(new Object[]{pre.getDate(),
                            pre.getMsisdn(),
                            pre.getCity(),
                            pre.getDistrict(),
                            current.getCity(),
                            current.getDistrict(),
                            pre.getTime(),
                            current.getTime(),
                            pre.getDuration(),
                            current.getDuration(),
                            current.getMoveTime()},ODSchemaProvider.OD_DISTRICT_SCHEMA_DET) ;
                    result.add(concatRow);
                    pre = current;
                } else {
                    pre = current;
                    continue;
                }

            }
        }
        return result;
    }

    @Data
    private class OdPoint implements Serializable{
        private Integer date;
        private String msisdn;
        private Timestamp time;
        private Integer city;
        private Integer district;
        private Integer duration;
        private Integer moveTime;
    }



}
