package com.sky.signal.pre.config;

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
     * 保存路径
     */
    private String savePath;

//    public static final String[] JS_CITY_CODES = new String[]{"1000250","1000510","1000511","1000512", "1000513","1000514","1000515","1000516","1000517","1000518","1000519","1000523","1000527"};

    private static String jsCitys = "citys";
    //全部地市
    private  String[] citys ;
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
     * OD文件
     */
    private String linkFile;

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

        if (args.containsOption(jsCitys)) {
            citys = args.getOptionValues(jsCitys).get(0).trim().split(",");
        }

        if (args.containsOption("mode")) {
            runMode = args.getOptionValues("mode").get(0).trim();
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
                    .concat(PathConfig.VALID_SIGNAL_PRO_PRE_SAVE_PATH)
                    .concat("*/")
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
                    .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                    .concat(PathConfig.VALID_SIGNAL_DISTRICT_PRE_SAVE_PATH)
                    .concat(districtCode).concat(java.io.File.separator)
                    .concat(day));
        }
        return fileList;
    }
    /**
     * 获取预处理后的全省基站文件保存路径
     */
    public String getCellSavePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.CELL_PATH);
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
                .concat(PathConfig.VALID_SIGNAL_DISTRICT_PRE_SAVE_PATH)
                .concat(districtCode)
                .concat(java.io.File.separator)
                .concat(date);
    }

    /**
     * 有效信令保存路径
     *
     * @return
     */
    public String getValidSignalSavePath1(String cityCde, String date) {
        return this.getBasePath()
                .concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.VALID_SIGNAL_SAVE_PATH)
                .concat(PathConfig.VALID_SIGNAL_PRO_PRE_SAVE_PATH)
                .concat(cityCde)
                .concat(java.io.File.separator)
                .concat(date);
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

    public String getPointPath(String day) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat(PathConfig.OD_POINT_SAVE_PATH).concat(day);
    }

    /**
     * 区县为单位分析基础od的时候，点位保存路径
     * @param districtCode
     * @param day
     * @return
     */
    public String getPointPath(String districtCode, String day) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.OD_POINT_SAVE_PATH)
                .concat(districtCode)
                .concat(java.io.File.separator)
                .concat(day);
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
    public String getProvinceTracePath(String date, String cityCde) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.PROVINCE_MSISDN_SIGNAL_SAVE_PATH)
                .concat(cityCde)
                .concat(java.io.File.separator)
                .concat(date);
    }

    public String getTraceSavePath(String cityCode, String date) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH)
                .concat(PathConfig.PROVINCE_MSISDN_SIGNAL_SAVE_PATH)
                .concat(cityCode)
                .concat(java.io.File.separator)
                .concat(date);
    }

    /**
     * 获取一天内在至少两个区县出现过的手机号码原始信令保存路径
     * @return
     */
    public List<String>  getProvinceSavePathTraceByCityCode(String cityCde) {
        String[] days = strDay.split(",", -1);
        List<String> fileList = new ArrayList<>();
        for (String day : days) {
            fileList.add(getProvinceTracePath(day,cityCde));
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
                .concat(PathConfig.PROVINCE_MSISDN_SAVE_PATH)
                .concat(date)
                .concat(java.io.File.separator);
    }
}
