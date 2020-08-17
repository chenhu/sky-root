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
     * 当前程序运行模式, province是以全省角度进行区县活动联系强度的预处理, district是以区县角度进行区县活动联系强度的预处理, common为普通程序
     */
    private String runMode;

    /**
     * 职住分析每个批次处理有效数据天数
     */
    private Integer workliveBatchSize;
    /**
     * 有效信令数据文件
     */
    private List<String> validSignalFilesForWorkLive;

    /**
     * 用来做工作地处理的有效信令文件
     */
    private List<String> validSignalForWork;
    /**
     * 用来做居住地处理的有效信令文件
     */
    private List<String> validSignalForLive;

    /**
     * 职住分析结果文件存储位置
     */
    private String workliveSavePath;

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
    private String SERVICENAME = "service";
    private static final Logger logger = LoggerFactory.getLogger
            (ParamProperties.class);

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
     *
     * @return
     */
    public List<String> getTraceFiles() {
        List<String> tracePathList = new ArrayList<>();
        String tracePath = this.getBasePath().concat(PathConfig.TRACE_PATH);
        String[] days = strDay.split(",");
        for (String day : days) {
            tracePathList.add(tracePath.concat(day));
        }
        return tracePathList;
    }

    /**
     * 获取当前要处理省轨迹数据路径列表
     * @param cityCode 城市代码
     * @return
     */
    public List<String> getTraceFiles(String cityCode) {
        List<String> tracePathList = new ArrayList<>();
        String tracePath = this.getBasePath().concat(PathConfig.TRACE_PATH);
        String[] days = strDay.split(",");
        for (String day : days) {
            tracePathList.add(tracePath.concat(day).concat(java.io.File.separator).concat(PathConfig.CITY_PRE_PATH).concat(cityCode).concat(java.io.File.separator));
        }
        return tracePathList;
    }

    /**
     * 获取指定日期所有地市下的轨迹数据
     * @param date 日期
     * @return
     */
    public String getTraceFiles(Integer date) {
        return this.getBasePath().concat(PathConfig.TRACE_PATH).concat(date.toString()).concat(java.io.File.separator).concat(PathConfig.CITY_PRE_PATH).concat("*").concat(java.io.File.separator);
    }

    /**
     * 获取当前要处理省轨迹数据路径列表
     * @param cityCode 城市代码
     * @param date 日期
     * @return
     */
    public String getTraceFiles(String cityCode, String date) {
        String tracePath = this.getBasePath().concat(PathConfig.TRACE_PATH);
        return tracePath.concat(date).concat(java.io.File.separator).concat(PathConfig.CITY_PRE_PATH).concat(cityCode).concat(java.io.File.separator);
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
                    .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                    .concat(day));
        }
        return fileList;
    }
    /**
     * 获取需要处理的有效信令路径列表
     * @param districtCode 区县编码
     * @return
     */
    public List<String> getValidSignalListByDays(String districtCode) {
        String[] days = strDay.split(",",-1);
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            fileList.add(getBasePath().concat(PathConfig.APP_SAVE_PATH)
                    .concat(PathConfig.VALID_SIGNAL_SAVE_PATH).concat(districtCode).concat(java.io.File.separator)
                    .concat(day));
        }
        return fileList;
    }

    /**
     * 获取需要处理的有效信令路径列表
     *
     * @return
     */
    public List<String> getValidSignalListByCityCodeAndDays(String cityCode) {
        String[] days = strDay.split(",");
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            fileList.add(getBasePath().concat(PathConfig.APP_SAVE_PATH)
                    .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                    .concat(cityCode)
                    .concat(java.io.File.separator)
                    .concat(day)
                    .concat(java.io.File.separator));
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
    public Map<String, Tuple2<String, List<String>>> getSignalFilePathTuple2() {
        String sep = java.io.File.separator;
        String[] days = strDay.split(",");
        Map<String, Tuple2<String, List<String>>> resultMap = new HashMap<>();
        for (String day : days) {
            resultMap.put(day, new Tuple2<>(getValidSignalSavePath(day), getTraceFiles("*")));
        }
        return resultMap;
    }


    /**
     * 获取预处理后的全省基站文件保存路径
     */
    public String getCellSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.CELL_PATH);
    }

    /**
     * 获取预处理后的枢纽基站文件保存路径
     */
    public String getTransCellSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.STATION_CELL_PATH);
    }

    /**
     * 获取全省基站的经纬度和geohash对照表文件路径
     *
     * @return
     */
    public String getGeoHashSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.GEOHASH_PATH);
    }

    /**
     * 用户CRM信息预处理后保存路径
     *
     * @return
     */
    public String getCRMSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.CRM_SAVE_PATH);
    }

    /**
     * 有效信令保存路径
     *
     * @return
     */
    public String getValidSignalSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.VALID_SIGNAL_SAVE_PATH);
    }

    /**
     * 有效信令保存路径
     *
     * @return
     */
    public String getValidSignalSavePath(String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                .concat(date)
                .concat(java.io.File.separator);
    }

    /**
     * 有效信令保存路径
     *
     * @return
     */
    public String getValidSignalSavePath(String districtCode, String date) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                .concat(districtCode)
                .concat(java.io.File.separator)
                .concat(date)
                .concat(java.io.File.separator);
    }

    /**
     * 原始基站文件路径
     * @return
     */
    public String getOriginCellPath() {
        return this.getBasePath().concat(PathConfig.ORN_CELL_PATH);
    }
    /**
     * OD trace
     * @return
     */
    public String getODTracePath(String day) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat(PathConfig.OD_TRACE_SAVE_PATH).concat(day);
    }
    /**
     * 基础OD结果
     * @return
     */
    public String getODResultPath(String districtCode, String day) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.OD_SAVE_PATH)
                .concat(districtCode)
                .concat(java.io.File.separator)
                .concat(day);
    }
    /**
     * 基础OD结果
     * @return
     */
    public String getODResultPath(String day) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.OD_SAVE_PATH)
                .concat(day);
    }
    /**
     * od分析中间统计结果
     * @return
     */
    public String getODStatTripPath(String day) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat(PathConfig.OD_STAT_TRIP_SAVE_PATH).concat(day);
    }

    /**
     * 获取区县原始信令保存路径
     * @param districtCode 区县编码
     * @param cityCode 城市编码
     * @param date 日期
     * @return
     */
    public String getDistrictTraceSavePath(Integer districtCode, String cityCode, String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.DISTRICT_SIGNAL_ALL_SAVE_PATH)
                .concat(districtCode.toString())
                .concat(java.io.File.separator)
                .concat(cityCode)
                .concat(java.io.File.separator)
                .concat(date);
    }

    /**
     * 获取一天内在至少两个区县出现过的手机号码原始信令保存路径
     * @param date 日期
     * @return
     */
    public String getPopulationTraceSavePath(String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.PROVINCE_MSISDN_SIGNAL_SAVE_PATH)
                .concat(date);
    }
    /**
     * 获取一天内在至少两个区县出现过的手机号码原始信令保存路径
     * @return
     */
    public List<String>  getPopulationTraceSavePath() {
        String[] days = strDay.split(",", -1);
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            fileList.add(getPopulationTraceSavePath(day));
        }
        return fileList;
    }


    /**
     * 获取区县原始信令路径列表
     * @param districtCode 区县编码
     * @return
     */
    public List<String> getDistrictTraceSavePath(Integer districtCode) {
        String[] days = strDay.split(",", -1);
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            fileList.add(getDistrictTraceSavePath(districtCode,"*",day));
        }
        return fileList;
    }

    /**
     *  区县中出现的手机号码持久化地址
     * @param districtCode
     * @param cityCode
     * @param date
     * @return
     */
    public String getDistrictMsisdnSavePath(Integer districtCode, String cityCode, String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.DISTRICT_MSISDN_SAVE_PATH)
                .concat(date)
                .concat(java.io.File.separator)
                .concat(cityCode)
                .concat(java.io.File.separator)
                .concat(districtCode.toString());
    }

    /**
     *  区县中出现的手机号码持久化地址
     * @param date 数据日期
     * @return
     */
    public String getDistrictMsisdnSavePath(String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.DISTRICT_MSISDN_SAVE_PATH)
                .concat(date)
                .concat(java.io.File.separator);
    }
}
