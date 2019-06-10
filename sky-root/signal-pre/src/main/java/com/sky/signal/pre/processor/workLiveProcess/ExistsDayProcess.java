package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;

/**
 * 从所有有效信令中统计每个手机号码出现天数
 */
@Service("existsDayProcess")
public class ExistsDayProcess implements Serializable {

    @Autowired
    private transient ParamProperties params;


    /**
     * 居住地判断处理器
     *
     */
    public void process(DataFrame validSignalDF, int batchId) {
        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        // 计算手机号码出现天数，以及每天逗留时间
        DataFrame existsDf = validSignalDF.groupBy("msisdn", "region", "cen_region", "sex", "age").
                agg(countDistinct("date").as("exists_days"), sum("move_time").as("sum_time")).orderBy("msisdn", "region", "cen_region", "sex", "age");
        FileUtil.saveFile(existsDf.repartition(partitions), FileUtil.FileType.CSV, params.getSavePath() + "exists-days/" + batchId + "/existsDf");
    }
}
