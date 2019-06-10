package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

@Service("liveWorkAggProcessor")
public class LiveWorkAggProcessor implements Serializable {

    // 出现天数阀值
    public static final int EXISTS_DAYS = 2;
    @Autowired
    private transient ParamProperties params;

    public DataFrame process() {
        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        // 计算手机号码出现天数，以及每天逗留时间
        DataFrame existsDf =  readExistsDf().persist(StorageLevel.DISK_ONLY());

        //出现2天的手机号
        DataFrame fitUsers = existsDf.filter(col("exists_days").geq(EXISTS_DAYS)).select(col("msisdn"));
        fitUsers = fitUsers.persist(StorageLevel.DISK_ONLY());

        //居住地处理
        DataFrame liveDfSumAll = readLiveDfSumAll();
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
        DataFrame liveDfUld = readDfUld();
        // 过滤出逗留时间超过2天的手机号码
        liveDfUld = liveDfUld.join(fitUsers, liveDfUld.col("msisdn").equalTo(fitUsers.col("msisdn"))).drop(fitUsers.col("msisdn"));
        liveDfUld = liveDfUld.persist(StorageLevel.DISK_ONLY());


        //工作地处理
        DataFrame workDfSumAll = readWorkDfSumAll();
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
        DataFrame workDfUwd = readDfUwd();
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

    private DataFrame readExistsDf() {
        DataFrame existsDf = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.EXIST_SCHEMA,params.getSavePath() + "exists-days/*/existsDf");
        existsDf = existsDf.groupBy("msisdn", "region", "cen_region", "sex", "age")
                .agg(sum("exists_days").as("exists_days"), sum("sum_time").as("sum_time"))
                .withColumn("stay_time", floor(col("sum_time").divide(col("exists_days"))).cast("Double"));
        return existsDf;
    }

    private DataFrame readLiveDfSumAll() {
        DataFrame liveDfSumAll = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_CELL_SCHEMA, params.getSavePath()
                + "live/*/liveDfSumAll");
        liveDfSumAll = liveDfSumAll.groupBy("msisdn", "base" ,"lng", "lat").agg(sum("stay_time").as("stay_time"), sum("days").as("days")).orderBy("msisdn", "base", "lng", "lat");
        return liveDfSumAll;
    }

    private DataFrame readWorkDfSumAll() {
        DataFrame workDfSumAll = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_CELL_SCHEMA, params.getSavePath()
                + "work/*/workDfSumAll");
        workDfSumAll = workDfSumAll.groupBy("msisdn", "base" ,"lng", "lat").agg(sum("stay_time").as("stay_time"), sum("days").as("days")).orderBy("msisdn", "base", "lng", "lat");
        return workDfSumAll;
    }

    private DataFrame readDfUld() {
        DataFrame liveDfUld = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.ULD_SCHEMA, params.getSavePath()
                + "live/*/liveDfUld");
        liveDfUld = liveDfUld.groupBy("msisdn").agg(sum("uld").as("uld"));
        return liveDfUld;
    }
    private DataFrame readDfUwd() {
        DataFrame workDfUld = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.UWD_SCHEMA, params.getSavePath()
                + "work/*/workDfUwd");
        workDfUld = workDfUld.groupBy("msisdn").agg(sum("uwd").as("uwd"));
        return workDfUld;
    }


}
