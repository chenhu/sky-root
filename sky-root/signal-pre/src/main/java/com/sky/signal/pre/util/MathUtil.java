package com.sky.signal.pre.util;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/9 14:53
 * description:
 */
public class MathUtil {
    //方差s^2=[(x1-x)^2 +...(xn-x)^2]/n
    public static double variance(Double[] x) {
        int m=x.length;
        double sum=0;
        for(int i=0;i<m;i++){//求和
            sum+=x[i];
        }
        double dAve=sum/m;//求平均值
        double dVar=0;
        for(int i=0;i<m;i++){//求方差
            dVar+=(x[i]-dAve)*(x[i]-dAve);
        }
        String formated = String.format("%.3f", dVar/m);
        return Double.valueOf(formated);
    }

    // 标准差
    public static double stdVariance(Double[] x) {
        return Math.sqrt(MathUtil.variance(x));
    }
}
