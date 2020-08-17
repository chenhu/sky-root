package com.sky.signal.population.config;

/**
 * Created by Chenhu on 2020/5/25.
 * 关于路径方面的配置，解决硬编码的问题
 */
public class PathConfig {
    //全省基站处理后保存文件夹名称
    public static final String CELL_PATH = "cell/";
    //当前处理区县基站文件保存文件夹名称，应该在"_"之前连接上当前区县编号
    public static final String DISTRICT_CELL_PATH = "_cell/";
    //基站经纬度和geohash对照表数据保存文件夹名称
    public static final String GEOHASH_PATH = "geohash/";
    //目标区县od点保存路径，应该在"_"之前连接上当前区县编号
    public static final String DEST_DISTRICT_OD_PATH = "_district_od/";
    //省级分析中，无逗留时间限制的区县od结果保存路径
    public static final String DEST_PROVINCE_OD_PATH = "province_od/";
    public static final String LIMITED_DISTRICT_OD_PATH = "_district_limited_od/";
    //省级分析中，有逗留时间限制的区县od结果保存路径
    public static final String LIMITED_PROVINCE_OD_PATH = "province_limited_od/";
    //处理过程中结果和中间数据保存路径
    public static final String APP_SAVE_PATH = "save/";

    //原始信令文件夹名称
    public static final String TRACE_PATH = "trace/dt=";
    //基础OD结果
    public static final String PROVINCE_MSISDN_OD_PATH = "od/";
    //人口迁徙分析目录
    public static final String POPULATION_DIR = "population/";
    //区县出行OD统计报表目录
    public static final String STAT_DIR = "_district_od_stat/";
    //省级区县出行OD统计报表目录
    public static final String PROVINCE_STAT_DIR = "province_od_stat/";




}
