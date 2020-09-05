package com.sky.signal.pre.processor.signalProcess;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.attribution.PhoneAttributionProcess;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.crmAnalyze.CRMProcess;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.MapUtil;
import com.sky.signal.pre.util.ProfileUtil;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 原始手机信令数据生成有效手机信令数据
 */
@Component
public class SignalProcessor implements Serializable {
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient SignalLoader signalLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient JavaSparkContext sparkContext;
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient CRMProcess crmProcess;
    @Autowired
    private transient PhoneAttributionProcess phoneAttributionProcess;

    /**
     * 合并同一手机连续相同基站信令数据
     */
    private static List<Row> mergeSameBase(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row prev = null;
        Row current = null;
        Timestamp begin_time = null;
        Timestamp last_time = null;
        for (int i = 0; i < rows.size(); i++) {
            if (prev == null) {
                prev = rows.get(i);
                begin_time = (Timestamp) prev.getAs("begin_time");
                last_time = (Timestamp) prev.getAs("last_time");
            } else {
                current = rows.get(i);
                if (prev.getAs("base").equals(current.getAs("base"))) {
                    last_time = (Timestamp) current.getAs("last_time");
                } else {
                    result.add(calcSignal(prev, current, begin_time, last_time));
                    prev = current;
                    begin_time = (Timestamp) prev.getAs("begin_time");
                    last_time = (Timestamp) prev.getAs("last_time");
                }
            }
        }
        //只有一笔
        if (prev != null && current == null) {
            result.add(calcSignal(prev, null, begin_time, last_time));
        }

        //最后一笔信令数据
        if (current != null) {
            result.add(calcSignal(current, null, begin_time, last_time));
        }

        return result;
    }

    /**
     * 计算信令数据 distance move_time speed
     */
    private static Row calcSignal(Row prior, Row current, Timestamp beginTime, Timestamp lastTime) {
        int distance = 0;
        int moveTime = (int) (lastTime.getTime() - beginTime.getTime()) / 1000;
        double speed = 0d;
        if (prior != null && current != null) {
            //基站与下一基站距离
            distance = MapUtil.getDistance((double) current.getAs("lng"), (double) current.getAs("lat"), (double) prior.getAs("lng"), (double) prior.getAs("lat"));
            //基站移动到下一基站速度
            speed = MapUtil.formatDecimal(moveTime == 0 ? 0 : distance / moveTime * 3.6, 2);
        }
        return new GenericRowWithSchema(new Object[]{prior.getAs("date"), prior.getAs("msisdn"), prior.getAs("region"), prior.getAs("city_code"), prior.getAs("district_code"), prior.getAs("tac"), prior.getAs("cell"), prior.getAs("base"), prior.getAs("lng"), prior.getAs("lat"), beginTime, lastTime, distance, moveTime, speed}, SignalSchemaProvider.SIGNAL_SCHEMA_BASE_1);
    }

    /**
     * 合并同一手机连续不同基站信令数据, 根据连续2/3/4/5笔处理不同的情况
     */
    private static List<Row> mergeDifferentBase(List<Row> rows) {
        List<Row> result = rows;
        int beforeRowCount, afterRowCount;
        //循环直到处理后减少的信令数据条数小于1%
        do {
            beforeRowCount = result.size();
            result = mergeDifferentBaseByRow2(result);
            result = mergeDifferentBaseByRow3(result);
            result = mergeDifferentBaseByRow4(result);
            result = mergeDifferentBaseByRow5(result);
            afterRowCount = result.size();
        } while ((beforeRowCount - afterRowCount) > (beforeRowCount / 100));
        return result;
    }

    /**
     * 合并同一手机连续2笔不同基站的信令数据
     * 情况1: 基站距离<=100, 或2个基站相同 将2笔合并到移动时间较长的那笔
     *
     * @param rows
     * @return
     */
    private static List<Row> mergeDifferentBaseByRow2(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row prev = null;
        Row current = null;
        Timestamp begin_time = null;
        Timestamp last_time = null;
        for (int i = 0; i < rows.size(); i++) {
            if (prev == null) {
                prev = rows.get(i);
                begin_time = (Timestamp) prev.getAs("begin_time");
            } else {
                current = rows.get(i);
                if ((int) prev.getAs("distance") < 100 || prev.getAs("base").equals(current.getAs("base"))) {
                    last_time = (Timestamp) current.getAs("last_time");
                    if ((int) prev.getAs("move_time") >= (int) current.getAs("move_time")) {
                        if (i + 1 < rows.size()) {
                            prev = calcSignal(prev, rows.get(i + 1), begin_time, last_time);
                        } else {
                            prev = calcSignal(prev, null, begin_time, last_time);
                        }
                    } else {
                        if (i + 1 < rows.size()) {
                            prev = calcSignal(current, rows.get(i + 1), begin_time, last_time);
                        } else {
                            prev = calcSignal(current, null, begin_time, last_time);
                        }
                    }
                } else {
                    result.add(prev);
                    prev = current;
                    begin_time = (Timestamp) prev.getAs("begin_time");
                }
            }
        }

        // 只有一笔 || 最后一笔信令数据
        if (prev != null) {
            result.add(prev);
        }

        return result;
    }

    /**
     * 合并同一手机连续3笔不同基站的信令数据 (A-B-C)
     * 情况1: A-C距离<=100，三种情况分别合并
     * 情况2: A-B距离移动时间正常(距离<=5000 或 移动时间>120), B-C距离移动时间不正常(距离>5000 并
     * 移动时间<120), 将C合并到B
     *
     * @param rows
     * @param row1
     * @param row2
     * @param row3
     * @param current
     * @return
     */
    private static Tuple3<Row, Row, Row> calcForRow3(List<Row> rows, Row row1, Row row2, Row row3, Row current) {
        if (row1 != null && row2 != null && row3 != null) {
            if (MapUtil.getDistance((double) row3.getAs("lng"), (double) row3.getAs("lat"), (double) row1.getAs("lng"), (double) row1.getAs("lat")) <= 100) {
                if ((int) row2.getAs("move_time") < 120 && (int) row1.getAs("move_time") + (int) row3.getAs("move_time") >= (int) row2.getAs("move_time")) {
                    row1 = calcSignal(row1, current, (Timestamp) row1.getAs("begin_time"), (Timestamp) row3.getAs("last_time"));
                    row2 = current;
                    row3 = null;
                } else if ((int) row1.getAs("move_time") < 120 && (int) row3.getAs("move_time") < 120 && (int) row1.getAs("move_time") + (int) row3.getAs("move_time") < (int) row2.getAs("move_time")) {
                    row1 = calcSignal(row2, current, (Timestamp) row1.getAs("begin_time"), (Timestamp) row3.getAs("last_time"));
                    row2 = current;
                    row3 = null;
                } else if ((int) row2.getAs("move_time") > 120) {
                    if ((int) row1.getAs("move_time") <= 120 && (int) row3.getAs("move_time") > 120) {
                        row1 = calcSignal(row2, row3, (Timestamp) row1.getAs("begin_time"), (Timestamp) row2.getAs("last_time"));
                        row2 = row3;
                        row3 = current;
                    } else if ((int) row1.getAs("move_time") > 120 && (int) row3.getAs("move_time") <= 120) {
                        row2 = calcSignal(row2, current, (Timestamp) row2.getAs("begin_time"), (Timestamp) row3.getAs("last_time"));
                        row3 = current;
                    } else {
                        rows.add(row1);
                        row1 = row2;
                        row2 = row3;
                        row3 = current;
                    }
                } else {
                    rows.add(row1);
                    row1 = row2;
                    row2 = row3;
                    row3 = current;
                }
            } else if (((int) row1.getAs("distance") <= 5000 || (int) row1.getAs("move_time") >= 120) && (int) row2.getAs("distance") > 5000 && (int) row2.getAs("move_time") < 120) {
                //第1/2笔距离<=5000 或 移动时间>120, 表示第1/2笔正常
                //第2/3笔距离>5000 并 第2/3笔移动时间<=120, 表示第3笔不正常, 将3合并到2
                row2 = calcSignal(row2, current, (Timestamp) row2.getAs("begin_time"), (Timestamp) row3.getAs("last_time"));
                row3 = current;
            } else {
                rows.add(row1);
                row1 = row2;
                row2 = row3;
                row3 = current;
            }
        }
        return new Tuple3<>(row1, row2, row3);
    }

    /**
     * 合并同一手机连续3笔不同基站的信令数据
     */
    private static List<Row> mergeDifferentBaseByRow3(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row row1 = null, row2 = null, row3 = null;

        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if (row1 == null) {
                row1 = current;
            } else if (row2 == null) {
                row2 = current;
            } else if (row3 == null) {
                row3 = current;
            } else {
                Tuple3<Row, Row, Row> tuple = calcForRow3(result, row1, row2, row3, current);
                row1 = tuple._1();
                row2 = tuple._2();
                row3 = tuple._3();
            }
        }
        //最后几笔
        Tuple3<Row, Row, Row> tuple = calcForRow3(result, row1, row2, row3, null);
        if (tuple._1() != null) result.add(tuple._1());
        if (tuple._2() != null) result.add(tuple._2());
        if (tuple._3() != null) result.add(tuple._3());

        return result;
    }

    /**
     * 合并同一手机连续4笔不同基站的信令数据
     * 情况1: A-B-C-D, A-D距离<=100，B+C移动时间<=120 并 A+D移动时间>=B+C移动时间, 将4笔合并到A
     *
     * @param rows
     * @param row1
     * @param row2
     * @param row3
     * @param row4
     * @param current
     * @return
     */
    private static Tuple4<Row, Row, Row, Row> calcForRow4(List<Row> rows, Row row1, Row row2, Row row3, Row row4, Row current) {
        if (row1 != null && row2 != null && row3 != null && row4 != null) {
            //第2+3笔移动时间<=120, 第1+4笔移动时间>=第2+3笔移动时间,
            // 第1/4笔基站相同或第1/4笔基站距离<=100米, 合并第2/3/4笔到第1笔
            if ((int) row2.getAs("move_time") + (int) row3.getAs("move_time") <= 120 && (int) row1.getAs("move_time") + (int) row4.getAs("move_time") >= (int) row2.getAs("move_time") + (int) row3.getAs("move_time") && MapUtil.getDistance((double) row1.getAs("lng"), (double) row1.getAs("lat"), (double) row4.getAs("lng"), (double) row4.getAs("lat")) <= 100) {
                row1 = calcSignal(row1, current, (Timestamp) row1.getAs("begin_time"), (Timestamp) row4.getAs("last_time"));
                row2 = current;
                row3 = null;
                row4 = null;
            } else {
                rows.add(row1);
                row1 = row2;
                row2 = row3;
                row3 = row4;
                row4 = current;
            }
        }
        return new Tuple4<>(row1, row2, row3, row4);
    }

    /**
     * 合并同一手机连续4笔不同基站的信令数据
     *
     * @param rows
     * @return
     */
    private static List<Row> mergeDifferentBaseByRow4(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row row1 = null, row2 = null, row3 = null, row4 = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if (row1 == null) {
                row1 = current;
            } else if (row2 == null) {
                row2 = current;
            } else if (row3 == null) {
                row3 = current;
            } else if (row4 == null) {
                row4 = current;
            } else {
                Tuple4<Row, Row, Row, Row> tuple = calcForRow4(result, row1, row2, row3, row4, current);
                row1 = tuple._1();
                row2 = tuple._2();
                row3 = tuple._3();
                row4 = tuple._4();
            }
        }
        Tuple4<Row, Row, Row, Row> tuple = calcForRow4(result, row1, row2, row3, row4, null);
        if (tuple._1() != null) result.add(tuple._1());
        if (tuple._2() != null) result.add(tuple._2());
        if (tuple._3() != null) result.add(tuple._3());
        if (tuple._4() != null) result.add(tuple._4());

        return result;
    }

    /**
     * 合并同一手机连续5笔不同基站的信令数据
     * 情况1: A-B-C-D-A, B+C+D移动时间<=120, 并 A+A移动时间>=B+C+D移动时间, 将5笔合并到A
     */
    private static Tuple5<Row, Row, Row, Row, Row> calcForRow5(List<Row> rows, Row row1, Row row2, Row row3, Row row4, Row row5, Row current) {
        if (row1 != null && row2 != null && row3 != null && row4 != null && row5 != null) {
            //2+3+4笔移动时间<=120, 第1+5笔移动时间>=第2+3+4笔移动时间,
            // 第1/5笔基站相同或第1/5笔基站距离<=100, 合并第2/3/4/5笔到第1笔
            if ((int) row2.getAs("move_time") + (int) row3.getAs("move_time") + (int) row4.getAs("move_time") <= 120 && (int) row1.getAs("move_time") + (int) row5.getAs("move_time") >= (int) row2.getAs("move_time") + (int) row3.getAs("move_time") + (int) row4.getAs("move_time") && MapUtil.getDistance((double) row1.getAs("lng"), (double) row1.getAs("lat"), (double) row5.getAs("lng"), (double) row5.getAs("lat")) <= 100) {
                row1 = calcSignal(row1, current, (Timestamp) row1.getAs("begin_time"), (Timestamp) row5.getAs("last_time"));
                row2 = current;
                row3 = null;
                row4 = null;
                row5 = null;
            } else {
                rows.add(row1);
                row1 = row2;
                row2 = row3;
                row3 = row4;
                row4 = row5;
                row5 = current;
            }
        }
        return new Tuple5<>(row1, row2, row3, row4, row5);
    }

    /**
     * 合并同一手机连续5笔不同基站的信令数据
     */
    private static List<Row> mergeDifferentBaseByRow5(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row row1 = null, row2 = null, row3 = null, row4 = null, row5 = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if (row1 == null) {
                row1 = current;
            } else if (row2 == null) {
                row2 = current;
            } else if (row3 == null) {
                row3 = current;
            } else if (row4 == null) {
                row4 = current;
            } else if (row5 == null) {
                row5 = current;
            } else {
                Tuple5<Row, Row, Row, Row, Row> tuple = calcForRow5(result, row1, row2, row3, row4, row5, current);
                row1 = tuple._1();
                row2 = tuple._2();
                row3 = tuple._3();
                row4 = tuple._4();
                row5 = tuple._5();
            }
        }
        Tuple5<Row, Row, Row, Row, Row> tuple = calcForRow5(result, row1, row2, row3, row4, row5, null);
        if (tuple._1() != null) result.add(tuple._1());
        if (tuple._2() != null) result.add(tuple._2());
        if (tuple._3() != null) result.add(tuple._3());
        if (tuple._4() != null) result.add(tuple._4());
        if (tuple._5() != null) result.add(tuple._5());

        return result;
    }

    /**
     * 合并移动时间小于5秒的信令数据
     * A-B-C, 如果B移动时间小于等于5秒, 则将B合并到A/C两笔中移动时间长的那笔
     */
    private static Tuple3<Row, Row, Row> calcForTransferSecs(List<Row> rows, Row row1, Row row2, Row row3, Row current) {
        if (row1 != null && row2 != null && row3 != null) {
            if ((int) row2.getAs("move_time") <= 5) {
                //将第2笔合并到第1/3笔中时间较长的那笔
                if ((int) row1.getAs("move_time") > (int) row3.getAs("move_time")) {
                    row1 = calcSignal(row1, row3, (Timestamp) row1.getAs("begin_time"), (Timestamp) row2.getAs("last_time"));
                    row2 = row3;
                    row3 = current;
                } else {
                    row2 = calcSignal(row3, current, (Timestamp) row2.getAs("begin_time"), (Timestamp) row3.getAs("last_time"));
                    row3 = current;
                }
            } else {
                rows.add(row1);
                row1 = row2;
                row2 = row3;
                row3 = current;
            }
        }
        return new Tuple3<>(row1, row2, row3);
    }

    /**
     * 合并移动时间小于5秒的信令数据
     * A-B-C, 如果B移动时间小于等于5秒, 则将B合并到A/C两笔中移动时间长的那笔
     *
     * @param rows
     * @return
     */

    private static List<Row> mergeByTransferSecs(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Row row1 = null, row2 = null, row3 = null;
        for (int i = 0; i < rows.size(); i++) {
            Row current = rows.get(i);
            if (row1 == null) {
                row1 = current;
            } else if (row2 == null) {
                row2 = current;
            } else if (row3 == null) {
                row3 = current;
            } else {
                Tuple3<Row, Row, Row> tuple = calcForTransferSecs(result, row1, row2, row3, current);
                row1 = tuple._1();
                row2 = tuple._2();
                row3 = tuple._3();
            }
        }
        Tuple3<Row, Row, Row> tuple = calcForTransferSecs(result, row1, row2, row3, null);
        if (tuple._1() != null) result.add(tuple._1());
        if (tuple._2() != null) result.add(tuple._2());
        if (tuple._3() != null) result.add(tuple._3());

        return result;
    }

    /**
     * 对一天的信令数据进行预处理
     */
    public void oneProcess(String path, final Broadcast<Map<String, Row>> cellVar, final Broadcast<Map<String, Row>> userVar, final Broadcast<Map<String, Row>> areaVar, final Broadcast<Map<Integer, Row>> regionVar) {

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
        //补全基站信息并删除重复信令
        DataFrame sourceDf = sqlContext.read().parquet(path).repartition(params.getPartitions());
        sourceDf = signalLoader.cell(cellVar).mergeCell(sourceDf).persist(StorageLevel.DISK_ONLY());
        //按手机号码对信令数据预处理
        JavaRDD<Row> rdd4 = SignalProcessUtil.signalToJavaPairRDD(sourceDf, params).values().flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new Function<Row, Timestamp>() {
                    @Override
                    public Timestamp apply(Row row) {
                        return (Timestamp) row.getAs("begin_time");
                    }
                });
                //按startTime排序
                rows = ordering.sortedCopy(rows);
                //合并同一手机连续相同基站信令数据
                rows = mergeSameBase(rows);

                //合并同一手机连续不同基站信令数据
                rows = mergeDifferentBase(rows);
                //合并移动时间小于5秒的信令数据
                rows = mergeByTransferSecs(rows);
                //合并同一手机连续不同基站信令数据
                rows = mergeDifferentBase(rows);
                return rows;
            }
        });
        DataFrame signalBaseDf = sqlContext.createDataFrame(rdd4, SignalSchemaProvider.SIGNAL_SCHEMA_BASE_1);
        signalBaseDf = signalBaseDf.persist(StorageLevel.DISK_ONLY());
        // 补全CRM数据、替换外省归属地
        JavaRDD<Row> signalBaseWithCRMRDD = signalLoader.crm(userVar).mergeCRM(signalBaseDf.javaRDD());
        DataFrame signalBaseWithCRMDf = sqlContext.createDataFrame(signalBaseWithCRMRDD, SignalSchemaProvider.SIGNAL_SCHEMA_BASE_2);
        // 补全归属地信息
        JavaRDD<Row> signalBaseWithRegionRDD = signalLoader.region(regionVar).mergeAttribution(signalBaseWithCRMDf.javaRDD());
        DataFrame signalMerged = sqlContext.createDataFrame(signalBaseWithRegionRDD, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA);
        //通过获取路径后8位的方式暂时取得数据日期，不从数据中获取
        String date = path.substring(path.length() - 8);
        FileUtil.saveFile(signalMerged.repartition(partitions), FileUtil.FileType.PARQUET, params.getValidSignalSavePath(date));
        signalBaseDf.unpersist();
    }

    /**
     * 手机信令数据预处理
     */
    public void process() {
        //普通基站信息
        final Broadcast<Map<String, Row>> cellVar = cellLoader.load(params.getCellSavePath());
        //CRM信息
        final Broadcast<Map<String, Row>> userVar = crmProcess.load();
        // 手机号码归属地信息
        final Broadcast<Map<Integer, Row>> regionVar = phoneAttributionProcess.process();
        //对轨迹数据预处理
        for (String traceFile : params.getTraceFiles("*")) {
            oneProcess(traceFile, cellVar, userVar, null, regionVar);
        }
    }
}
