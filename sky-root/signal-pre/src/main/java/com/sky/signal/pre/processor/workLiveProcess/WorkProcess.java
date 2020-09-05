package com.sky.signal.pre.processor.workLiveProcess;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;

/**
 * 工作地判断处理器
 */
@Service("workProcess")
public class WorkProcess implements Serializable {
    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient ParamProperties params;


    private static final Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Timestamp>() {
        @Override
        public Timestamp apply(Row row) {
            return (Timestamp) row.getAs("begin_time");
        }
    });

    private static final LocalTime time0900 = new LocalTime(9, 0);
    private static final LocalTime time1130 = new LocalTime(11, 30);
    private static final LocalTime time1400 = new LocalTime(14, 0);
    private static final LocalTime time1630 = new LocalTime(16, 30);

    /**
     * 工作地判断
     *
     */
    private List<Row> getWorkData(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Iterator<Row> iterator = rows.iterator();
        Row prior = null;
        while (iterator.hasNext()) {
            Row current = iterator.next();
            DateTime tmpDateTime = new DateTime(current.getAs("begin_time"));
            // 工作地判断，排除周末数据和节假日数据
            if (tmpDateTime.getDayOfWeek() == DateTimeConstants.SATURDAY || tmpDateTime.getDayOfWeek() == DateTimeConstants.SUNDAY) {
                continue;
            }
            if (prior == null) {
                prior = current;
            } else {
                DateTime begin_time1 = new DateTime(prior.getAs("begin_time"));
                DateTime begin_time2 = new DateTime(current.getAs("begin_time"));

                LocalDate date1 = begin_time1.toLocalDate();
                LocalDate date2 = begin_time2.toLocalDate();
                LocalTime time1 = begin_time1.toLocalTime();
                LocalTime time2 = begin_time2.toLocalTime();

                if (date1.equals(date2)) { //同1天
                    if ((time1.compareTo(time0900) < 0 && time2.compareTo(time0900) < 0) ||
                            (time1.compareTo(time1130) >= 0 && time2.compareTo(time1130) >= 0)) {
                        //如果时间都小于9:00或大于11:00, 数据无效
                    } else {
                        if (time1.compareTo(time0900) < 0) {
                            begin_time1 = begin_time1.withTime(time0900);
                        }
                        if (time2.compareTo(time1130) >= 0) {
                            begin_time2 = begin_time2.withTime(time1130);
                        }
                        double stayTime = Math.abs(Seconds.secondsBetween(begin_time1, begin_time2).getSeconds());
                        result.add(RowFactory.create(Integer.valueOf(begin_time1.toString("yyyyMMdd")), prior.getAs("msisdn"), prior.getAs("base"), prior.getAs("lng"), prior.getAs("lat"), new Timestamp(begin_time1.getMillis()), new Timestamp(begin_time2.getMillis()), stayTime));
                    }
                    if ((time1.compareTo(time1400) < 0 && time2.compareTo(time1400) < 0) ||
                            (time1.compareTo(time1630) >= 0 && time2.compareTo(time1630) >= 0)) {
                        //如果时间都小于14:00或大于16:30, 数据无效
                    } else {
                        if (time1.compareTo(time1400) < 0) {
                            begin_time1 = begin_time1.withTime(time1400);
                        }
                        if (time2.compareTo(time1630) >= 0) {
                            begin_time2 = begin_time2.withTime(time1630);
                        }
                        double stayTime = Math.abs(Seconds.secondsBetween(begin_time1, begin_time2).getSeconds());
                        result.add(RowFactory.create(Integer.valueOf(begin_time1.toString("yyyyMMdd")), prior.getAs("msisdn"), prior.getAs("base"), prior.getAs("lng"), prior.getAs("lat"), new Timestamp(begin_time1.getMillis()), new Timestamp(begin_time2.getMillis()), stayTime));
                    }
                }
                prior = current;
            }
        }
        return result;
    }

    /**
     * 工作地判断处理器
     *
     */
    public void process(DataFrame validSignalDF, Integer batchId) {
        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        //手机号码->信令数据
        JavaPairRDD<String, Row> signalRdd = validSignalDF.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                String msisdn = (String) row.getAs("msisdn");
                return new Tuple2<>(msisdn, row);
            }
        });
        //按手机号对数据分组，相同手机号放到同一个List里面
        List<Row> rows = new ArrayList<>();
        JavaPairRDD<String, List<Row>> signalPairRdd = signalRdd.aggregateByKey(rows, params.getPartitions(), new Function2<List<Row>,
                Row, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows, Row row) throws Exception {
                rows.add(row);
                return rows;
            }
        }, new Function2<List<Row>, List<Row>, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows1, List<Row> rows2) throws Exception {
                rows1.addAll(rows2);
                return rows1;
            }
        });

        JavaRDD<List<Row>> signalListRdd = signalPairRdd.values();
        JavaRDD<Row> workRdd = signalListRdd.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                //按startTime排序
                rows = ordering.sortedCopy(rows);
                rows = getWorkData(rows);
                return rows;
            }
        });
        //工作地明细数据
        DataFrame workDf = sqlContext.createDataFrame(workRdd, LiveWorkSchemaProvider.BASE_SCHEMA);
        workDf = workDf.persist(StorageLevel.DISK_ONLY());
        //按基站加总
        DataFrame workDfSumAll = workDf.groupBy("msisdn", "base", "lng", "lat").agg(sum("stay_time").as("stay_time"), countDistinct("date").as("days")).orderBy("msisdn", "base", "lng", "lat");
        FileUtil.saveFile(workDfSumAll.repartition(partitions),FileUtil.FileType.PARQUET,params.getWorkSumAllSavePath(batchId.toString()));
        DataFrame workDfUwd = workDf.groupBy("msisdn").agg(countDistinct("date").as("uwd"));
        FileUtil.saveFile(workDfUwd.repartition(partitions),FileUtil.FileType.PARQUET,params.getWorkUwdSavePath(batchId.toString()));
        validSignalDF.unpersist();
        workDf.unpersist();
    }
}
