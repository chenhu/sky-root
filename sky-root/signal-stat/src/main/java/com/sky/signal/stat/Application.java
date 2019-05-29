package com.sky.signal.stat;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.service.ComputeService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.Assert;

/**
 * 入口类
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder().sources(Application.class).web(false).run(args)) {
            ParamProperties params = context.getBean(ParamProperties.class);

            //计算服务名称参数
            Assert.hasLength(params.getService(), "计算服务名称不可为空");

            //获取计算服务
            ComputeService computeService = (ComputeService) context.getBean(params.getService());
            Assert.notNull(computeService, "获取计算服务为空: " + params.getService());

            //计算
            computeService.compute();
        }
    }
}
