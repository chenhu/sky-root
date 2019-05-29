package com.sky.signal.pre.util;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 16:30
 * description: 获取当前profile环境配置
 */
@Component
public class ProfileUtil implements ApplicationContextAware {
    private static ApplicationContext context = null;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.context = applicationContext;
    }

    public static String getActiveProfile() {
        String[] profiles = context.getEnvironment().getActiveProfiles();
        if( ! ArrayUtils.isEmpty(profiles)){
            return profiles[0];
        }
        return "";
    }
}
