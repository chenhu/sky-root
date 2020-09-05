package com.sky.signal.pre.processor.workLiveProcess;

import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.MapUtil;
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
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.Seconds;
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
 * 居住地判断处理器
 */
@Service
public class LiveProcess implements Serializable {
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

    private static final LocalTime time0100 = new LocalTime(1, 0);
    private static final LocalTime time0700 = new LocalTime(7, 0);

    /**
     * 居住地判断
     *
     */
    private List<Row> getLiveData(List<Row> rows) {
        List<Row> result = new ArrayList<>();
        Iterator<Row> iterator = rows.iterator();
        Row prior = null;
        while (iterator.hasNext()) {
            Row current = iterator.next();
            if (prior == null) {
                prior = current;
            } else {
                DateTime begin_time1 = new DateTime(prior.getAs("begin_time"));
                DateTime begin_time2 = new DateTime(current.getAs("begin_time"));
                LocalDate date1 = begin_time1.toLocalDate();
                LocalDate date2 = begin_time2.toLocalDate();
                LocalTime time1 = begin_time1.toLocalTime();
                LocalTime time2 = begin_time2.toLocalTime();

                if (date1.equals(date2)) {  //同1天
                    if ((time1.compareTo(time0100) < 0 && time2.compareTo(time0100) < 0) || (time1.compareTo(time0700) >= 0 && time2.compareTo(time0700) >= 0)) {
                        //如果时间都小于1:00或大于7:00, 数据无效
                    } else {
                        if (time1.compareTo(time0100) < 0) {
                            begin_time1 = begin_time1.withTime(time0100);
                        }
                        if (time2.compareTo(time0700) >= 0) {
                            begin_time2 = begin_time2.withTime(time0700);
                        }
                        double stayTime = Math.abs(Seconds.secondsBetween(begin_time1, begin_time2).getSeconds());
                        result.add(RowFactory.create(Integer.valueOf(begin_time1.toString("yyyyMMdd")), prior.getAs("msisdn"), prior.getAs("base"), prior.getAs("lng"), prior.getAs("lat"), new Timestamp(begin_time1.getMillis()), new Timestamp(begin_time2.getMillis()), stayTime));
                    }
                } else if (date1.plusDays(1).equals(date2)) { //隔1天
                    //后1天1:00前没数据, 并且两个基站距离<=500
                    if (time2.compareTo(time0100) >= 0 && MapUtil.getDistance((double) prior.getAs("lng"), (double) prior.getAs("lat"), (double) current.getAs("lng"), (double) current.getAs("lat")) <= 500) {
                        begin_time1 = begin_time1.withDate(date2).withTime(time0100);
                        if (time2.compareTo(time0700) >= 0) {
                            begin_time2 = begin_time2.withTime(time0700);
                        }
                        double stayTime = Math.abs(Seconds.secondsBetween(begin_time1, begin_time2).getSeconds());
                        result.add(RowFactory.create(Integer.valueOf(begin_time2.toString("yyyyMMdd")), current.getAs("msisdn"), current.getAs("base"), current.getAs("lng"), current.getAs("lat"), new Timestamp(begin_time1.getMillis()), new Timestamp(begin_time2.getMillis()), stayTime));
                    }
                }
                prior = current;
            }
        }
        return result;
    }
    /**
     * 居住地判断处理器
     *
     */
    public void process(DataFrame validSignalDF, int batchId) {
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

        //居住地处理
        JavaRDD<Row> rdd4 = signalListRdd.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                //按startTime排序
                rows = ordering.sortedCopy(rows);
                rows = getLiveData(rows);
                return rows;
            }
        });
        DataFrame liveDf = sqlContext.createDataFrame(rdd4, LiveWorkSchemaProvider.BASE_SCHEMA);
        liveDf = liveDf.persist(StorageLevel.DISK_ONLY());
        //按基站加总
        DataFrame liveDfSumAll = liveDf.groupBy("msisdn", "base" ,"lng", "lat").agg(sum("stay_time").as("stay_time"), countDistinct("date").as("days")).orderBy("msisdn", "base", "lng", "lat");
        FileUtil.saveFile(liveDfSumAll.repartition(partitions),FileUtil.FileType.CSV,params.getSavePath()+"live/" + batchId + "/liveDfSumAll");

        DataFrame liveDfUld = liveDf.groupBy("msisdn").agg(countDistinct("date").as("uld"));
        FileUtil.saveFile(liveDfUld.repartition(partitions),FileUtil.FileType.CSV,params.getSavePath()+"live/" + batchId + "/liveDfUld");
        validSignalDF.unpersist();
        liveDf.unpersist();
    }
}
