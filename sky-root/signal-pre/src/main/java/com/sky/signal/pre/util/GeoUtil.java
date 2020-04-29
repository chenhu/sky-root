package com.sky.signal.pre.util;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2020/4/25 15:37
 * description:
 */
public class GeoUtil {

    public static String geo(Double lat, Double lng) {
        if(lat == null || lng == null) {
            return null;
        }
        GeoHash liveGeoHash = new GeoHash(lat, lng);
        liveGeoHash.sethashLength(7);
        return liveGeoHash.getGeoHashBase32();
    }
}
