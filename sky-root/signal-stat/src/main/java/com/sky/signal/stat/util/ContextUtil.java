package com.sky.signal.stat.util;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * 获取ApplicationContext
 */
@Component
public class ContextUtil implements ApplicationContextAware {
	private static ApplicationContext applicationContext;

	/**
	 * 实现ApplicationContextAware接口的回调方法，设置上下文环境
	 * @param applicationContext 上下文
	 * @throws BeansException 异常
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		ContextUtil.applicationContext = applicationContext;
	}

	/**
	 * @return ApplicationContext 上下文
	 */
	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}
}
