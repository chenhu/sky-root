package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
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
import org.joda.time.Seconds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@Service("liveWorkAggProcessor")
public class LiveWorkAggProcessor implements Serializable {

    // 出现天数阀值
    public static final int EXISTS_DAYS = 2;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient WorkProcess workProcess;
    @Autowired
    private transient LiveProcess liveProcess;

    /**
     * 数据跨天的拆成2笔
     */
    private DataFrame expandLastTime(DataFrame df) {
        JavaRDD<Row> rdd = df.javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                List<Row> rows = new ArrayList<>();
                Integer move_time1;
                Integer move_time2;
                Double speed1;
                Double speed2;
                DateTime begin_time = new DateTime(row.getAs("begin_time"));
                DateTime last_time = new DateTime(row.getAs("last_time"));
                LocalDate date1 = begin_time.toLocalDate();
                LocalDate date2 = last_time.toLocalDate();
                if (!date1.equals(date2)) {
                    Timestamp time1 = new Timestamp(last_time.dayOfWeek().roundFloorCopy().getMillis() - 1000);
                    Timestamp time2 = new Timestamp(last_time.dayOfWeek().roundFloorCopy().getMillis());
                    move_time1 = Math.abs(Seconds.secondsBetween(begin_time, new DateTime(time1)).getSeconds());
                    move_time2 = Math.abs(Seconds.secondsBetween(new DateTime(time2), last_time).getSeconds());
                    Integer distance = row.getAs("distance");
                    speed1 = MapUtil.formatDecimal(move_time1 == 0 ? 0 : distance / move_time1 * 3.6, 2);
                    speed2 = MapUtil.formatDecimal(move_time2 == 0 ? 0 : distance / move_time2 * 3.6, 2);
                    Integer date = Integer.valueOf(row.getAs("date").toString());
                    rows.add(RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("region"), row.getAs("city_code"), row
                            .getAs("cen_region"),
                            row.getAs("sex"), row.getAs("age"), row.getAs("tac"), row.getAs("cell"), row.getAs("base"),
                            row.getAs("lng"), row.getAs("lat"), row.getAs("begin_time"), time1, distance, move_time1, speed1));
                    rows.add(RowFactory.create(date + 1, row.getAs("msisdn"), row.getAs("region"), row.getAs("city_code"), row.getAs
                            ("cen_region"),
                            row.getAs("sex"), row.getAs("age"), row.getAs("tac"), row.getAs("cell"), row.getAs("base"),
                            row.getAs("lng"), row.getAs("lat"), time2, row.getAs("last_time"), distance, move_time2, speed2));
                } else {
                    rows.add(row);
                }
                return rows;
            }
        });
        return sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA);
    }

    public DataFrame process() {

        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }


        DataFrame validSignalDF = null;
        for (String ValidSignalFile : params.getValidSignalFilesForWorkLive()) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile);
            if (validSignalDF == null) {
                validSignalDF = validDF;
            } else {
                validSignalDF = validSignalDF.unionAll(validDF);
            }
        }

        // 跨天信令拆分为两条
        DataFrame validDf = expandLastTime(validSignalDF);

        validDf = validDf.repartition(params.getPartitions()).persist(StorageLevel.DISK_ONLY());
        // 计算手机号码出现天数，以及每天逗留时间
        DataFrame existsDf = validDf.groupBy("msisdn", "region", "cen_region", "sex", "age").
                agg(countDistinct("date").as("exists_days"), sum("move_time").as("sum_time"));
        FileUtil.saveFile(existsDf.repartition(partitions), FileUtil.FileType.CSV, params.getSavePath() + "work_live/existsDf");
        existsDf =  existsDf.persist(StorageLevel.DISK_ONLY());
        //手机号码->信令数据
        JavaPairRDD<String, Row> signalRdd = validDf.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                String msisdn = row.getAs("msisdn");
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

        //要做2次计算, 所以先持久化
        JavaRDD<List<Row>> signalListRdd = signalPairRdd.values();
        signalListRdd = signalListRdd.persist(StorageLevel.DISK_ONLY());

        liveProcess.process(signalListRdd);
        workProcess.process(signalListRdd);

        //总的出现天数
        existsDf = existsDf.groupBy("msisdn", "region", "cen_region", "sex", "age").
                agg(sum("exists_days").as("exists_days"), sum("sum_time").as("sum_time")).withColumn("stay_time", floor(col("sum_time")
                .divide(col("exists_days"))).cast("Double"));
        //出现2天的手机号
        DataFrame fitUsers = existsDf.filter(col("exists_days").geq(EXISTS_DAYS)).select(col("msisdn"));
        fitUsers = fitUsers.persist(StorageLevel.DISK_ONLY());

        //居住地处理
        DataFrame liveDfSumAll = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_CELL_SCHEMA, params.getSavePath()
                + "work_live/liveDfSumAll");
        // 计算手机号码居住地基站逗留时间、逗留天数、平均每日逗留时间
        liveDfSumAll = liveDfSumAll.groupBy("msisdn", "base", "lng", "lat").agg(sum("stay_time").as("stay_time"), sum("days").as("days"))
                .withColumn("daily_time", floor(col("stay_time").divide(col("days"))));
        liveDfSumAll = liveDfSumAll.persist(StorageLevel.DISK_ONLY());
        // 计算手机号码居住地最大逗留时间
        DataFrame liveDfMaxStayTime = liveDfSumAll.groupBy("msisdn").agg(max("stay_time").as("stay_time"));

        // 找出手机号码在居住地 发生最大逗留时间的 基站
        DataFrame liveDfOnLsd = liveDfSumAll.join(liveDfMaxStayTime, liveDfSumAll.col("msisdn").equalTo(liveDfMaxStayTime.col("msisdn"))
                .and(liveDfSumAll.col("stay_time").equalTo(liveDfMaxStayTime.col("stay_time"))))
                .select(liveDfSumAll.col("msisdn"), liveDfSumAll.col("base"), liveDfSumAll.col("lng"), liveDfSumAll.col("lat"),
                        liveDfSumAll.col("daily_time"), liveDfSumAll.col("days").as("on_lsd"));

        // 过滤发生最大逗留时间 基站中，逗留时间超过2小时的基站
        liveDfOnLsd = liveDfOnLsd.filter(col("daily_time").gt(7200));
        // 找出 出现天数超过2天的手机号码中， 最大逗留时间超过2小时的基站
        liveDfOnLsd = liveDfOnLsd.join(fitUsers, liveDfOnLsd.col("msisdn").equalTo(fitUsers.col("msisdn"))).drop(fitUsers.col("msisdn"));
        liveDfOnLsd = liveDfOnLsd.persist(StorageLevel.DISK_ONLY());

        // 加载手机号码、居住地逗留天数 的数据
        DataFrame liveDfUld = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.ULD_SCHEMA, params.getSavePath()
                + "work_live/liveDfUld");
        // 过滤出逗留时间超过2天的手机号码
        liveDfUld = liveDfUld.join(fitUsers, liveDfUld.col("msisdn").equalTo(fitUsers.col("msisdn"))).drop(fitUsers.col("msisdn"));
        liveDfUld = liveDfUld.persist(StorageLevel.DISK_ONLY());


        //工作地处理
        DataFrame workDfSumAll = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_CELL_SCHEMA, params
                .getSavePath() + "work_live/workDfSumAll");
        //计算手机号码工作地基站逗留时间、逗留天数、平均每日逗留时间
        workDfSumAll = workDfSumAll.groupBy("msisdn", "base", "lng", "lat").agg(sum("stay_time").as("stay_time"), sum("days").as("days"))
                .withColumn("daily_time", floor(col("stay_time").divide(col("days"))));
        workDfSumAll = workDfSumAll.persist(StorageLevel.DISK_ONLY());

        // 计算手机号码工作地最大逗留时间
        DataFrame wordDfMaxStayTime = workDfSumAll.groupBy("msisdn").agg(max("stay_time").as("stay_time"));
        // 找出手机号码在工作地 发生最大逗留时间的 基站
        DataFrame workDfOnWsd = workDfSumAll.join(wordDfMaxStayTime, workDfSumAll.col("msisdn").equalTo(wordDfMaxStayTime.col("msisdn"))
                .and(workDfSumAll.col("stay_time").equalTo(wordDfMaxStayTime.col("stay_time"))))
                .select(workDfSumAll.col("msisdn"), workDfSumAll.col("base"), workDfSumAll.col("lng"), workDfSumAll.col("lat"),
                        workDfSumAll.col("daily_time"), workDfSumAll.col("days").as("on_wsd"));

        // 过滤手机号码在工作地 发生最大逗留时间的 基站中，逗留时间超过2小时的基站
        workDfOnWsd = workDfOnWsd.filter(col("daily_time").gt(7200));
        // 找出 出现天数超过2天的手机号码中， 最大逗留时间超过2小时的基站
        workDfOnWsd = workDfOnWsd.join(fitUsers, workDfOnWsd.col("msisdn").equalTo(fitUsers.col("msisdn"))).drop(fitUsers.col("msisdn"));
        workDfOnWsd = workDfOnWsd.persist(StorageLevel.DISK_ONLY());

        // 加载手机号码、工作地逗留天数 的数据
        DataFrame workDfUwd = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.UWD_SCHEMA, params.getSavePath()
                + "work_live/workDfUwd");

        // 加载手机号码、工作地逗留天数 的数据
        workDfUwd = workDfUwd.groupBy("msisdn").agg(sum("uwd").as("uwd"));
        // 过滤出逗留时间超过2天的手机号码
        workDfUwd = workDfUwd.join(fitUsers, workDfUwd.col("msisdn").equalTo(fitUsers.col("msisdn"))).drop(fitUsers.col("msisdn"));
        workDfUwd = workDfUwd.persist(StorageLevel.DISK_ONLY());

        DataFrame result = existsDf.join(liveDfOnLsd, existsDf.col("msisdn").equalTo(liveDfOnLsd.col("msisdn")), "left_outer")
                .select(existsDf.col("msisdn"),
                        existsDf.col("region"),
                        existsDf.col("cen_region"),
                        existsDf.col("sex"),
                        existsDf.col("age"),
                        existsDf.col("exists_days"),
                        existsDf.col("stay_time"),
                        liveDfOnLsd.col("base").as("live_base"),
                        liveDfOnLsd.col("lng").as("live_lng"),
                        liveDfOnLsd.col("lat").as("live_lat"),
                        liveDfOnLsd.col("on_lsd"));

        result = result.join(liveDfUld, result.col("msisdn").equalTo(liveDfUld.col("msisdn")), "left_outer")
                .select(result.col("msisdn"),
                        result.col("region"),
                        result.col("cen_region"),
                        result.col("sex"),
                        result.col("age"),
                        result.col("exists_days"),
                        result.col("stay_time"),
                        result.col("live_base"),
                        result.col("live_lng"),
                        result.col("live_lat"),
                        result.col("on_lsd"),
                        liveDfUld.col("uld"));

        result = result.join(workDfOnWsd, result.col("msisdn").equalTo(workDfOnWsd.col("msisdn")), "left_outer")
                .select(result.col("msisdn"),
                        result.col("region"),
                        result.col("cen_region"),
                        result.col("sex"),
                        result.col("age"),
                        result.col("exists_days"),
                        result.col("stay_time"),
                        result.col("live_base"),
                        result.col("live_lng"),
                        result.col("live_lat"),
                        result.col("on_lsd"),
                        result.col("uld"),
                        workDfOnWsd.col("base").as("work_base"),
                        workDfOnWsd.col("lng").as("work_lng"),
                        workDfOnWsd.col("lat").as("work_lat"),
                        workDfOnWsd.col("on_wsd"));

        result = result.join(workDfUwd, result.col("msisdn").equalTo(workDfUwd.col("msisdn")), "left_outer")
                .select(result.col("msisdn"),
                        result.col("region"),
                        result.col("cen_region"),
                        result.col("sex"),
                        result.col("age"),
                        result.col("stay_time"),
                        result.col("exists_days"),
                        result.col("live_base"),
                        result.col("live_lng"),
                        result.col("live_lat"),
                        result.col("on_lsd"),
                        result.col("uld"),
                        result.col("work_base"),
                        result.col("work_lng"),
                        result.col("work_lat"),
                        result.col("on_wsd"),
                        workDfUwd.col("uwd"));
        //排除结果中，一个人多个职住地的情况
        result = result.dropDuplicates(new String[]{"msisdn"});
        FileUtil.saveFile(result.repartition(partitions), FileUtil.FileType.CSV, params.getWorkLiveFile());

        existsDf.unpersist();
        fitUsers.unpersist();

        liveDfSumAll.unpersist();
        liveDfUld.unpersist();
        liveDfOnLsd.unpersist();

        workDfSumAll.unpersist();
        workDfUwd.unpersist();
        workDfOnWsd.unpersist();
        return result;
    }
}
