package com.sky.signal.pre.config;

/**
 * Created by Chenhu on 2020/5/25.
 * 关于路径方面的配置，解决硬编码的问题
 */
public class PathConfig {
    //原始基站文件路径
    public static final String ORN_CELL_PATH = "cell/cell.csv";
    //普通基站处理后保存文件夹名称
    public static final String CELL_PATH = "cell";
    //枢纽基站处理后保存文件夹名称
    public static final String STATION_CELL_PATH = "station_cell";
    //基站经纬度和geohash对照表数据保存文件夹名称
    public static final String GEOHASH_PATH = "geohash";
    //枢纽站预处理后带停留点类型的文件保存文件夹名称
    public static final String STATION_DATA_PATH = "station-data/";

    //枢纽站预处理后带停人口分类的文件保存文件夹名称
    public static final String STATION_CLASSIC_PATH = "station-classic/";

    //职住分析结果保存文件夹名称
    public static final String WORKLIVE_PATH = "work_live_his";

    //处理过程中结果和中间数据保存路径
    public static final String APP_SAVE_PATH = "save/";
    //原始信令文件夹名称
    public static final String TRACE_PATH = "trace/dt=";
    public static final String CITY_PRE_PATH = "city=";
    //CRM预处理后的保存路径
    public static final String CRM_SAVE_PATH = "crm/";
    //有效信令的保存路径
    public static final String VALID_SIGNAL_SAVE_PATH = "validSignal/";
    //OD 轨迹
    public static final String OD_TRACE_SAVE_PATH = "od_trace/";
    //基础OD结果
    public static final String OD_SAVE_PATH = "od/";
    //OD统计中间结果
    public static final String OD_STAT_TRIP_SAVE_PATH = "stat_trip/";
    //从全省原始信令中抽取在指定区县出现的手机号码的原始信令，保存原始信令的位置
    public static final String DISTRICT_SIGNAL_ALL_SAVE_PATH = "trace/district=";



}
