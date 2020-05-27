package com.sky.signal.pre.config;

/**
 * Created by Chenhu on 2020/5/25.
 * 关于路径方面的配置，解决硬编码的问题
 */
public class PathConfig {
    //普通基站处理后保存文件夹名称
    public static final String CELL_PATH = "cell";
    //枢纽基站处理后保存文件夹名称
    public static final String STATION_CELL_PATH = "station_cell";
    //基站经纬度和geohash对照表数据保存文件夹名称
    public static final String GEOHASH_PATH = "geohash";
    //枢纽站预处理后保存文件夹名称
    public static final String STATION_DATA_PATH = "station-data/";
}
