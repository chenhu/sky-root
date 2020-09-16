package com.sky.signal.stat.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties("param")
public class ParamProperties {
    /**
     * spark应用名称
     */
    private String appName;

    /**
     * spark主机url
     */
    private String masterUrl;

    /**
     * 分区数
     */
    private Integer partitions;

    /**
     * 计算服务名称
     */
    private String service;

    /**
    * description: 当前处理的地市
    * param:
    * return:
    **/
    private Integer cityCode;

    /**
     * 移动信令数据基础路径
     */
    private String basePath;

    private Integer statBatchSize;

    /**
     * 有效信令数据文件
     */
    private List<String> validSignalFilesForWorkLive;

    /**
     * 用来做统计分析的有效信令
     */
    private List<String> validSignalDate;

    /**
     * 统计报表输出的分区数
     */
    private Integer statpatitions;

    /**
     * 职住文件
     */
    private String workLiveName;

    /**
     * 职住文件1
     */
    private String workLiveFile1;
    /**
     * 职住文件2
     */
    private String workLiveFile2;
    /**
     * 停驻点
     */
    private String traceFile;

    /**
     * OD文件
     */
    private List<String> odDate;

    /**
     * OD trace文件
     */
    private List<String> odTraceDate;

    /**
     * 出行统计的文件列表
     */
    private List<String> odTripStatDate;

    /**
     * 分区数
     */
    public static final String PARTITIONS = "partitions";

    /**
     * 处理日期
     */
//    private int year, month, day;
    public static final String YEAR = "year", MONTH = "month", DAY = "day";
    private String strYear, strMonth, strDay;
    // 服务名称注入
    private String SERVICENAME = "service";
    // 如果数据量比较大，分批次进行处理，每个批次处理的数据"份数"
    private String BATCH_SIZE = "batches";

    // 统计报表最终输出文件数
    private String STAT_PARTIONS = "stat-partition";

    private static final Logger logger = LoggerFactory.getLogger(ParamProperties.class);

    /**
     * 注入程序参数
     *
     * @param args
     */
    @Autowired
    public void setArgs(ApplicationArguments args) {
        // 注入当前要运行的服务名称
        if (args.containsOption(SERVICENAME)) {
            service = args.getOptionValues(SERVICENAME).get(0).trim();
        }

        //通过程序参数指定分区数: --partitions=100
        if (args.containsOption(PARTITIONS)) {
            partitions = Integer.valueOf(args.getOptionValues(PARTITIONS).get(0).trim());
        } else {
            logger.warn("Has not passing [PARTITIONS] argument, the partitions will be use the default value {}", partitions);
        }

        //通过程序参数指定数据年份: --year=2017
        if (args.containsOption(YEAR)) {
            strYear = args.getOptionValues(YEAR).get(0).trim();
//            year = Integer.valueOf(strYear);
        }

        //通过程序参数指定数据月份: --month=9
        if (args.containsOption(MONTH)) {
            if(args.containsOption(YEAR)) {
                strMonth = args.getOptionValues(MONTH).get(0).trim();
//                month = Integer.valueOf(strMonth);
            } else {
                throw new RuntimeException("Should passing the [year] argument first if you want to use the [month] argument , using: --year --month ");
            }
        }

        //通过程序参数指定数据日期的天部分: --day=24,25,26 ,通过逗号分割的天
        if (args.containsOption(DAY)) {
            if(args.containsOption(MONTH)) {
                strDay = args.getOptionValues(DAY).get(0).trim();
//                day = Integer.valueOf(strDay);
            } else {
                throw new RuntimeException("Should passing the [month] argument first if you want to use the [day] argument , using: --year --month --day");
            }
        }
        if (args.containsOption(BATCH_SIZE)) {
            statBatchSize = Integer.valueOf(args.getOptionValues(BATCH_SIZE).get(0).trim());
        }

        if (args.containsOption(STAT_PARTIONS)) {
            statpatitions = Integer.valueOf(args.getOptionValues(STAT_PARTIONS).get(0).trim());
        }
    }

    public String getBaseHourSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.BASE_HOUR_STAT).concat(batchid).concat(java.io.File.separator);
    }
    public String getBaseHourSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.BASE_HOUR_STAT);
    }

    public String getMigrateSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.MiGRATE_STAT).concat(batchid).concat(java.io.File.separator);
    }
    public String getMigrateSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.MiGRATE_STAT);
    }

    public String getCombineOdSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.COMBINE_OD).concat(batchid).concat(java.io.File.separator);
    }
    public String getCombineOdSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.COMBINE_OD);
    }

    public String getOdDaySavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_DAY_STAT);
    }
    public String getODTimeIntervalGeneralSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_TIME_INTERVAL_GENERAL_STAT);
    }
    public String getODTimeDistanceSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_TIME_DISTANCE_STAT);
    }

    public String getODTraceDaySavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.OD_TRACE_DAY).concat(batchid).concat(java.io.File.separator);
    }
    public String getODTraceDaySavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_TRACE_DAY);
    }

    public String getODTraceBusyTimeSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.OD_TRACE_BUSY_TIME_DAY).concat(batchid).concat(java.io.File.separator);
    }
    public String getODTraceBusyTimeSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_TRACE_BUSY_TIME_DAY);
    }

    public String getODTripStatSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.OD_TRIP_STAT).concat(batchid).concat(java.io.File.separator);
    }
    public String getODTripStatSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_TRIP_STAT);
    }

    public String getODTripClassStatSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.OD_TRIP_CLASS_STAT).concat(batchid).concat(java.io.File.separator);
    }
    public String getODTripClassStatSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.OD_TRIP_CLASS_STAT);
    }

    public String getValidSignalStatSavePath(String batchid) {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.STAT_TMP).concat(PathConfig.VALIDSIGNAL_STAT).concat(batchid).concat(java.io.File.separator);
    }
    public String getValidSignalStatSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.VALIDSIGNAL_STAT);
    }

    public String getWorkLiveStatSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.WORK_LIVE_STAT);
    }
    public String getPersonClassStat1SavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.PERSON_CLASS_STAT1);
    }
    public String getPersonClassStat2SavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.PERSON_CLASS_STAT2);
    }

    public String getDayTripSummaryStatSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.DAY_TRIP_SUMMARY_STAT);
    }
    public String getDayTripSummaryWithPurposeStatSavePath() {
        return this.getBasePath().concat(PathConfig.STAT_ROOT).concat(this.getCityCode().toString()).concat(PathConfig.DAY_TRIP_WITH_PURPOSE_SUMMARY_STAT);
    }
    public String getWorkLiveFile() {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.WORK_LIVE_SAVE_PATH)
                .concat(this.getWorkLiveName());
    }
    public String getValidSignalSavePath(String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                .concat(date);
    }
    public List<String> getValidSignalFilesForStat() {
        List<String> result = new ArrayList<>();
        for(String day: this.getValidSignalDate()) {
            result.add(this.getValidSignalSavePath(day));
        }
        return result;
    }

    public String getODTracePath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_TRACE_SAVE_PATH)
                .concat(day);
    }

    public List<String> getODTracePath() {
        List<String> result = new ArrayList<>();
        for(String day: this.getOdTraceDate()) {
            result.add(this.getODTracePath(day));
        }
        return result;
    }
    public String getODStatTripPath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_STAT_TRIP_SAVE_PATH)
                .concat(day);
    }
    public List<String> getODStatTripPath() {
        List<String> result = new ArrayList<>();
        for(String day: this.getOdTripStatDate()) {
            result.add(this.getODStatTripPath(day));
        }
        return result;
    }

    public String getODResultPath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_SAVE_PATH)
                .concat(day);
    }
    public List<String> getODResultPath() {
        List<String> result = new ArrayList<>();
        for(String day: this.getOdDate()) {
            result.add(this.getODResultPath(day));
        }
        return result;
    }
}
