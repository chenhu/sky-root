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

    /**
     * 基站数据文件
     */
    private String cellFile;

    /**
    * description: 当前处理的地市
    * param:
    * return:
    **/
    private Integer cityCode;

    /**
     * 手机号段归属地数据文件
     */
    private String phoneCityFile;

    /**
     * 移动信令数据基础路径
     */
    private String basePath;

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
     * 职住文件
     */
    private String workLiveFile;

    /**
     * 停驻点
     */
    private String traceFile;

    /**
     * OD文件
     */
    private String linkFile;

    /**
     * 基站区域对照表
     */
    private String baseFile;

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
//    private int year, month, day;
    public static final String YEAR = "year", MONTH = "month", DAY = "day";
    private String strYear, strMonth, strDay;
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
    }

    /**
     * 获取客户hdfs上面原始信令数据路径
     * 目前客户hdfs上面文件路径为 basepath/YYYYMMDD/xxxx.gz
     * 此方法后面要根据实际情况作改动
     * @return
     */
    public List<String> getTraceSignalFileFullPath() {
        String orignal = this.getBasePath();
        String sep = java.io.File.separator;
        String[] days = strDay.split(",");
        List<String> fileList = new ArrayList<>();
        for (String day: days) {
            if(orignal.endsWith(sep)) {
                fileList.add(orignal + "track_" + strYear + strMonth + day);
            } else {
                fileList.add(orignal + sep + "track_" + strYear + strMonth + day);
            }
        }
        return fileList;
    }

    /**
    * description: 获取有效信令文件路径
    * param: []
    * return: java.lang.String
    **/
    public List<String> getValidSignalFileFullPath() {
        String orignal = this.getSavePath();
        String sep = java.io.File.separator;
        String[] days = strDay.split(",");
        List<String> fileList = new ArrayList<>();
        for (String day: days) {
            if(orignal.endsWith(sep)) {
                fileList.add(orignal + "validSignal" + sep + strYear + strMonth + day);
            } else {
                fileList.add(orignal + sep + "validSignal" + sep + strYear + strMonth + day);
            }
        }
        return fileList;
    }
}
