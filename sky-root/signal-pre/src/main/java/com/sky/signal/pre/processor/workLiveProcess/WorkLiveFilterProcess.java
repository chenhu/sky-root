package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.MapUtil;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.Seconds;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/6/3 15:47
 * description: 把要用来作为职住分析的有效数据，事先过滤一遍，然后持久化，减少职住分析时候的资源占用
 */
@Service
public class WorkLiveFilterProcess implements Serializable {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    private static final LocalTime time0100 = new LocalTime(1, 0);
    private static final LocalTime time0700 = new LocalTime(7, 0);

    private static final LocalTime time0900 = new LocalTime(9, 0);
    private static final LocalTime time1130 = new LocalTime(11, 30);
    private static final LocalTime time1400 = new LocalTime(14, 0);
    private static final LocalTime time1630 = new LocalTime(16, 30);
    public void process() {
        for (String ValidSignalFile : params.getValidSignalFilesForWorkLive()) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile);
            // 获取当前有效数据的日期
            String[] path = ValidSignalFile.split("/");
            String date  = path[path.length -1];
            filterLiveData(validDF, date);
            filterWorkData(validDF, date);
        }
    }
    private void filterLiveData(DataFrame validDF, String date) {
            final int day = Integer.valueOf(date.substring(7,8));
            // 居住地有效数据过滤
            JavaRDD<Row> liveRdd = validDF.toJavaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row row) throws Exception {
                    DateTime begin_time = new DateTime(row.getAs("begin_time"));
                    LocalDate date = begin_time.toLocalDate();
                    int currentDay =  date.getDayOfMonth();
                    LocalTime time = begin_time.toLocalTime();
                    if(currentDay == day) {
                        return time.compareTo(time0100) >= 0 && time.compareTo(time0700) <= 0;
                    } else {
                        return time.compareTo(time0100) >= 0;
                    }
                }
            });
            int partitions = 1;
            if(!ProfileUtil.getActiveProfile().equals("local")) {
                partitions = params.getPartitions();
            }
            DataFrame liveDf = sqlContext.createDataFrame(liveRdd, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA);
            liveDf = expandLastTime(liveDf);
            FileUtil.saveFile(liveDf.repartition(partitions),FileUtil.FileType.CSV,params.getSavePath()+"validSignalForLive/" + date);
    }

    private void filterWorkData(DataFrame validDF, String date) {
        // 工作地有效数据过滤
        JavaRDD<Row> workRdd = validDF.toJavaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                DateTime begin_time = new DateTime(row.getAs("begin_time"));
                LocalTime time = begin_time.toLocalTime();
                return (time.compareTo(time0900) >= 0 && time.compareTo(time1130) <= 0) || (time.compareTo(time1400) >= 0 && time.compareTo(time1630) <= 0);
            }
        });
        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        DataFrame workDf = sqlContext.createDataFrame(workRdd, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA);

        workDf = expandLastTime(workDf);
        FileUtil.saveFile(workDf.repartition(partitions),FileUtil.FileType.CSV,params.getSavePath()+"validSignalForWork/" + date);
    }

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
}
