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
    //基站经纬度和geohash对照表数据保存文件夹名称
    public static final String GEOHASH_PATH = "geohash";
    //处理过程中结果和中间数据保存路径
    public static final String APP_SAVE_PATH = "save/";
    //原始信令文件夹名称
    public static final String TRACE_PATH = "trace/dt=";
    public static final String CITY_PRE_PATH = "city=";
    //有效信令的保存路径
    public static final String VALID_SIGNAL_SAVE_PATH = "validSignal/";
    //分地市存储有效信令
    public static final String VALID_SIGNAL_PRO_PRE_SAVE_PATH = "province/";
    //分区县存储有效信令
    public static final String VALID_SIGNAL_DISTRICT_PRE_SAVE_PATH = "district/";
    //OD 轨迹
    public static final String OD_TRACE_SAVE_PATH = "od_trace/";
    //基础OD结果
    public static final String OD_SAVE_PATH = "od/";
    //OD统计中间结果
    public static final String OD_STAT_TRIP_SAVE_PATH = "stat_trip/";
    //用于做区县od的停留点保存路径
    public static final String OD_POINT_SAVE_PATH = "point/";
    //从全省原始信令中抽取在指定区县出现的手机号码的原始信令，保存原始信令的位置
    public static final String DISTRICT_SIGNAL_ALL_SAVE_PATH = "trace/district=";
    //从全省原始信令中抽取一天内在至少两个区县出现的手机号码的原始信令，保存原始信令的位置
    public static final String PROVINCE_MSISDN_SIGNAL_SAVE_PATH = "trace/province/";
    public static final String DISTRICT_MSISDN_SAVE_PATH = "msisdn/";
    public static final String PROVINCE_MSISDN_SAVE_PATH = "msisdn/province/";
    public static final String DISTRICT_MSISDN_COUNT_SAVE_PATH = "stat_district_msisdn_count/";



}
