package com.sky.signal.pre.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * 保存路径
     */
    private String savePath;

    private String phoneCityFile;

    public static final String[] JS_CITY_CODES = new String[]{"1000250","1000510","1000511","1000512",
            "1000513","1000514","1000515","1000516","1000517","1000518","1000519","1000523","1000527"};

    /**
     * 移动信令数据基础路径
     */
    private String basePath;

    /**
     * 要处理区县所在的地市编码
     */
    private Integer cityCode;

    /**
     * 当前要分析区县的编码，用于取得当前区县的信令
     */
    private Integer districtCode;
    /**
     * 职住分析每个批次处理有效数据天数
     */
    private Integer workliveBatchSize;
    /**
     * 有效信令数据文件
     */
    private List<String> signalsForWorkLive;

    /**
     * 用来做工作地处理的有效信令文件
     */
    private List<String> validSignalForWork;

    // 给职住分析结果取名字，方便统计分析中根据名称使用不同的职住分析结果
    private String workLiveName;
    /**
     * 用来做居住地处理的有效信令文件
     */
    private List<String> validSignalForLive;
    /**
     * 停驻点
     */
    private String traceFile;

    /**
     * OD文件
     */
    private String linkFile;

    /**
     * 当处理的信令是在一个区域内的时候，指定的一个区域内的基站信息
     */
    private String specifiedAreaBaseFile;

    /**
     * 枢纽分析中虚拟基站的经纬度，以及虚拟基站的Base(tac|cell)
     */
    private Double visualLng;
    private Double visualLat;
    private String visualStationBase;
    /**
     * 用户信息
     */
    private String userFile;
    /**
     * 分区数
     */
    public static final String PARTITIONS = "partitions";
    /**
     * 处理日期
     */
    public static final String DAY = "day";
    private String strDay;
    // 服务名称注入
    private static final String SERVICENAME = "service";
    private static final Logger logger = LoggerFactory.getLogger
            (ParamProperties.class);

    /**
     * 注入程序参数
     *
     * @param args 注入的参数
     */
    @Autowired
    public void setArgs(ApplicationArguments args) {
        // 注入当前要运行的服务名称
        if (args.containsOption(SERVICENAME)) {
            service = args.getOptionValues(SERVICENAME).get(0).trim();
        }
        if (args.containsOption("districtCode")) {
            districtCode = Integer.valueOf(args.getOptionValues("districtCode").get(0)) ;
        }
        if (args.containsOption("cityCode")) {
            cityCode = Integer.valueOf(args.getOptionValues("cityCode").get(0).trim());
        }

        //通过程序参数指定分区数: --partitions=100
        if (args.containsOption(PARTITIONS)) {
            partitions = Integer.valueOf(args.getOptionValues(PARTITIONS).get
                    (0).trim());
        } else {
            logger.warn("Has not passing [PARTITIONS] argument, the " +
                    "partitions will be use the default value {}", partitions);
        }

        //通过程序参数指定数据日期的天部分: --day=24,25,26 ,通过逗号分割的天
        if (args.containsOption(DAY)) {
            strDay = args.getOptionValues(DAY).get(0).trim();
        }
    }
    /**
     * 获取当前要处理省轨迹数据路径列表
     * @return
     */
    public List<String> getTraceFiles() {
        List<String> tracePathList = new ArrayList<>();
        String[] days = strDay.split(",");
        for (String day : days) {
            tracePathList.add(this.getBasePath()
                    .concat(this.getCityCode().toString())
                    .concat(java.io.File.separator)
                    .concat(PathConfig.TRACE_PATH)
                    .concat(day));
        }
        return tracePathList;
    }
    public String getTraceFilesByDay(String day) {
        return this.getBasePath()
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.TRACE_PATH)
                .concat(day);
    }

    /**
     * 获取需要处理的有效信令路径列表
     *
     * @return
     */
    public List<String> getValidSignalListByDays() {
        String[] days = strDay.split(",",-1);
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            fileList.add(getBasePath().concat(PathConfig.APP_SAVE_PATH)
                    .concat(this.getCityCode().toString())
                    .concat(java.io.File.separator)
                    .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                    .concat(day));
        }
        return fileList;
    }
    /**
     * description: 获取枢纽有效信令文件路径,文件内容带有停留点类型
     * param: []
     * return: java.lang.String
     **/
    public List<String> getStationTraceFileFullPath() {
        String orignal = this.getSavePath();
        String sep = java.io.File.separator;
        String[] days = strDay.split(",");
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            if (orignal.endsWith(sep)) {
                fileList.add(orignal + PathConfig.STATION_DATA_PATH + day);
            } else {
                fileList.add(orignal + sep + PathConfig.STATION_DATA_PATH + day);
            }
        }
        return fileList;
    }


    /**
     * description: 返回根据日期参数拼接成的 日期、有效数据路径、原始数据路径 的 map
     * param: []
     * return: java.util.Map<java.lang.String,scala.Tuple2<java.lang.String,
     * java.lang.String>>
     **/
    public Map<String, Tuple2<String, String>> getSignalFilePathTuple2() {
        String sep = java.io.File.separator;
        String[] days = strDay.split(",");
        Map<String, Tuple2<String, String>> resultMap = new HashMap<>();
        for (String day : days) {
            resultMap.put(day, new Tuple2<>(getValidSignalSavePath(day), getTraceFilesByDay(day)));
        }
        return resultMap;
    }
    public String getDataQualitySavePath(String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.DATA_QUALITY_SAVE_PATH)
                .concat(date);
    }
    public String getDataQualityAllSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.DATA_QUALITY_ALL_SAVE_PATH);
    }
    /**
     * 获取预处理后的全省基站文件保存路径
     */
    public String getCellSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.CELL_PATH);
    }

    /**
     * 获取预处理后的枢纽基站文件保存路径
     */
    public String getTransCellSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat(this.getCityCode().toString()).concat(java.io.File.separator).concat
                (PathConfig.STATION_CELL_PATH);
    }

    /**
     * 获取全省基站的经纬度和geohash对照表文件路径
     *
     * @return
     */
    public String getGeoHashSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.GEOHASH_PATH);
    }

    /**
     * 用户CRM信息预处理后保存路径
     *
     * @return
     */
    public String getCRMSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.CRM_SAVE_PATH);
    }
    /**
     * 有效信令保存路径
     *
     * @return
     */
    public String getValidSignalSavePath(String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                .concat(date);
    }

    /**
     * 原始基站文件路径
     * @return
     */
    public String getOriginCellPath() {
        return this.getBasePath()
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.ORN_CELL_PATH);
    }
    /**
     * OD trace
     * @return
     */
    public String getODTracePath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_TRACE_SAVE_PATH)
                .concat(day);
    }
    /**
     * 基础OD结果
     * @return
     */
    public String getODResultPath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_SAVE_PATH)
                .concat(day);
    }
    /**
     * od分析中间统计结果
     * @return
     */
    public String getODStatTripPath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_STAT_TRIP_SAVE_PATH)
                .concat(day);
    }
    public String getPointPath(String day) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.OD_POINT_SAVE_PATH)
                .concat(day);
    }

    public String getExistsDaysSavePath(String batch) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.EXISTS_DAYS_SAVE_PATH)
                .concat(batch).concat(java.io.File.separator)
                .concat("tmp");
    }
    public String getLiveSumAllSavePath(String batch) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.LIVE_SAVE_PATH)
                .concat(batch)
                .concat(java.io.File.separator)
                .concat(PathConfig.SUM_ALL);
    }
    public String getLiveUldSavePath(String batch) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.LIVE_SAVE_PATH)
                .concat(batch)
                .concat(java.io.File.separator)
                .concat(PathConfig.ULD);
    }
    public String getWorkSumAllSavePath(String batch) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.WORK_SAVE_PATH)
                .concat(batch)
                .concat(java.io.File.separator)
                .concat(PathConfig.SUM_ALL);
    }
    public String getWorkUwdSavePath(String batch) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.WORK_SAVE_PATH)
                .concat(batch)
                .concat(java.io.File.separator)
                .concat(PathConfig.UWD);
    }
    public String getWorkLiveSavePath() {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(this.getCityCode().toString())
                .concat(java.io.File.separator)
                .concat(PathConfig.WORK_LIVE_SAVE_PATH)
                .concat(this.getWorkLiveName());
    }

    public List<String> getValidSignalFilesForWorkLive() {
        List<String> result = new ArrayList<>();
        for(String day: this.getSignalsForWorkLive()) {
            result.add(this.getValidSignalSavePath(day));
        }
        return result;
    }

}
