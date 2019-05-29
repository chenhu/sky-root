package com.sky.signal.stat.util;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.math.BigDecimal;

public class MapUtil {
    /**
     * 地球半径, 单位米
     */
    public static final double EARTH_RADIUS = 6378137;

    private MapUtil() {
    }

    /**
     * 浮点数精度, 四舍五入
     *
     * @param value
     * @param scale
     * @return
     */
    public static double formatDecimal(double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 角度弧度计算公式
     *
     * @param degree
     * @return
     */
    private static double getRadian(double degree) {
        return degree * Math.PI / 180.0;
    }

    private static Column getRadian(Column degree) {
        return degree.multiply(Math.PI).divide(180.0);
    }

    /**
     * 根据经纬度计算两点之间距离
     *
     * @param lng1
     * @param lat1
     * @param lng2
     * @param lat2
     * @return 两点之间距离 单位米
     */
    public static int getDistance(double lng1, double lat1, double lng2, double lat2) {
        double latRad1 = getRadian(lat1);
        double latRad2 = getRadian(lat2);
        // 两点纬度差
        double latSub = latRad1 - latRad2;
        // 两点经度差
        double lngSub = getRadian(lng1) - getRadian(lng2);
        double s = Math.asin(Math.sqrt(Math.pow(Math.sin(latSub / 2), 2) + Math.cos(latRad1) * Math.cos(latRad2) * Math.pow(Math.sin(lngSub / 2), 2))) * 2;
        return (int) Math.round(s * EARTH_RADIUS);
    }

    public static Column getDistance(Column lng1, Column lat1, Column lng2, Column lat2) {
        Column latRad1 = getRadian(lat1);
        Column latRad2 = getRadian(lat2);
        // 两点纬度差
        Column latSub = latRad1.minus(latRad2);
        // 两点经度差
        Column lngSub = getRadian(lng1).minus(getRadian(lng2));
        Column s = functions.asin(functions.sqrt(functions.pow(functions.sin(latSub.divide(2)), 2).plus(functions.cos(latRad1).multiply(functions.cos(latRad2)).multiply(functions.pow(functions.sin(lngSub.divide(2)), 2))))).multiply(2);
        return functions.round(s.multiply(EARTH_RADIUS));
    }
}
