package com.sky.signal.stat.util;

/**
 * Created by ChenHu <chenhu1008@me.com> on 2020/3/25.
 */

/**
 * 人口类别分类器接口，通过传入 出现天数 和 一天在指定基站停留的时间，返回人员类别，可能为
 * 常住人口、旅游商务人口、流动人口、其他 等等。
 */
public interface PersonClassification {
    /**
     * 分类
     * @param days 出现天数
     * @param seconds 一天在指定基站停留的时间(秒)
     * @return 类别
     */
    int classify(Long days, Double seconds );
    int classify(Long uld, Integer stay_time_class);
}
