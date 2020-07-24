package com.sky.signal.population.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import scala.Tuple2;

import java.util.*;

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
     * 基站数据文件
     */
    private String cellFile;
    /**
     * 当前处理的地市编码
     */
    private Integer cityCode;

    /**
     * 当前处理的区县编码
     */
    private String districtCode;
    /**
     * 区域OD分析判定方式
     * odMode = 0, 只要在区县有停留点，则算一次OD出行，不用满足停留时间满足阀值
     * odMode = 1, 只有在区县停留时间满足阀值，才算一次OD出行
     */
    private Integer odMode;
    /**
     * 移动信令数据基础路径
     */
    private String basePath;

    /**
     * 停驻点
     */
    private String traceFile;

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
    //    private int year, month, day;
    public static final String YEAR = "year", MONTH = "month", DAY = "day";
    private String strYear, strMonth, strDay;
    // 服务名称注入
    private String SERVICENAME = "service";
    private static final Logger logger = LoggerFactory.getLogger
            (ParamProperties.class);

    // 全省轨迹日期,一般是逗号分割的日期，比如: --odays=20190611,20190618
    private static final String ORGINAL_DATE = "odays";
    private List<String> odays;

    /**
     * 注入程序参数
     *
     * @param args
     */
    @Autowired
    public void setArgs(ApplicationArguments args) {
        // 注入当前要运行的服务名称
        if (args.containsOption(ORGINAL_DATE)) {
            odays = Arrays.asList(args.getOptionValues(ORGINAL_DATE).get(0)
                    .trim().split(","));
        }
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

        //通过程序参数指定数据年份: --year=2017
        if (args.containsOption(YEAR)) {
            strYear = args.getOptionValues(YEAR).get(0).trim();
            //            year = Integer.valueOf(strYear);
        }

        //通过程序参数指定数据月份: --month=9
        if (args.containsOption(MONTH)) {
            if (args.containsOption(YEAR)) {
                strMonth = args.getOptionValues(MONTH).get(0).trim();
                //                month = Integer.valueOf(strMonth);
            } else {
                throw new RuntimeException("Should passing the [year] " +
                        "argument first if you want to use the [month] " +
                        "argument , using: --year --month ");
            }
        }

        //通过程序参数指定数据日期的天部分: --day=24,25,26 ,通过逗号分割的天
        if (args.containsOption(DAY)) {
            if (args.containsOption(MONTH)) {
                strDay = args.getOptionValues(DAY).get(0).trim();
                //                day = Integer.valueOf(strDay);
            } else {
                throw new RuntimeException("Should passing the [month] " +
                        "argument first if you want to use the [day] argument" +
                        " , using: --year --month --day");
            }
        }


    }

    /**
     * 获取预处理后的全省基站文件路径
     *
     * @return
     */
    public String getValidProvinceCellPath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.CELL_PATH);
    }

    /**
     * 获取预处理后的指定区县基站文件路径
     *
     * @param districtCode 区县编码
     * @return
     */
    public String getCurrentDistrictCellPath(String districtCode) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (districtCode).concat(PathConfig.DISTRICT_CELL_PATH);
    }

    /**
     * 获取全省基站的经纬度和geohash对照表文件路径
     *
     * @return
     */
    public String getGeoHashFilePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (PathConfig.GEOHASH_PATH);
    }

    /**
     * 获取区县OD分析保存路径
     *
     * @param districtCode 区县编码
     * @return
     */
    public String getDestDistrictOdFilePath(String districtCode) {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat
                (districtCode).concat(PathConfig.DEST_DESTRICT_OD_PATH);
    }

    /**
     * 获取全省不分区县OD保存路径
     *
     * @return
     */
    public String getProvinceODFilePath() {
        return this.getBasePath().concat(PathConfig.APP_SAVE_PATH).concat(PathConfig.PROVINCE_MSISDN_OD_PATH);
    }

    /**
     * 获取当前要处理日期的全省轨迹数据路径列表
     * @return
     */
    public List<String> getProvinceTraceFilePath() {
        List<String> tracePathList = new ArrayList<>();
        String tracePath = this.getBasePath().concat(PathConfig.TRACE_PATH);
        if(!CollectionUtils.isEmpty(this.odays)) {
            for(String oday: odays) {
                tracePathList.add(tracePath.concat(oday));
            }
        } else {
            throw new IllegalArgumentException("需要指定要处理的轨迹日期， " +
                    "--odays=yyyyMMdd, yyyyMMdd");
        }
        return tracePathList;
    }
}
