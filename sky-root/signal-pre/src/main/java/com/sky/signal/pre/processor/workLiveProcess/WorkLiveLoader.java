package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * 加载职住分析的数据
 */
@Service("workLiveLoader")
public class WorkLiveLoader implements Serializable {
    @Autowired
    private transient ParamProperties params;

    public DataFrame load() {
        return FileUtil.readFile(FileUtil.FileType.PARQUET, LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, params.getWorkLiveSavePath()).repartition(params.getPartitions());
    }
}
