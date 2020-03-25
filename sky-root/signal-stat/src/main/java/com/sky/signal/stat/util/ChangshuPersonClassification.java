package com.sky.signal.stat.util;

/**
 * Created by ChenHu <chenhu1008@me.com> on 2020/3/25.
 */

/**
 * 常熟需求中用到的人口分类器
 * 将人口类别分为 0-常住人口 1-流动人口 2-其他
 */
public  class ChangshuPersonClassification implements PersonClassification {
    private static final Integer dayFlag = 7;
    private static final Integer secondsFlag = 480*60;

    @Override
    public int classify( Long days, Double seconds) {
        Integer person_class;
        if (days >= dayFlag && seconds > secondsFlag) { //常住人口
            person_class = 0;
        } else if (days < dayFlag && days > 1 && seconds > secondsFlag) { // 流动人口
            person_class = 1;
        } else { // 其他人口
            person_class = 2;
        }
        return person_class;
    }
}
