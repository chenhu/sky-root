package com.sky.signal.stat.processor.od;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.signal.SignalSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;

/**
 * 加载OD数据
 */
@Service("odLoader")
public class ODLoader implements Serializable {
    @Autowired
    private transient ParamProperties params;
    public DataFrame loadOD() {
        List<String> odFiles = params.getOdFiles();
        DataFrame odDf = null ;
        for (String odFile: odFiles) {
            if(odDf == null) {
                odDf = FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_SCHEMA, odFile);
            } else {
                odDf = odDf.unionAll(FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_SCHEMA, odFile));
            }
        }
        return odDf.repartition(params.getPartitions());
    }

    public DataFrame loadODTrace() {
        List<String> odTraceFiles = params.getOdTraceFiles();
        DataFrame odTraceDf = null ;
        for (String odTraceFile: odTraceFiles) {
            if(odTraceDf == null) {
                odTraceDf = FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_TRACE_SCHEMA, odTraceFile);
            } else {
                odTraceDf = odTraceDf.unionAll(FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_TRACE_SCHEMA, odTraceFile));
            }
        }
        return odTraceDf.repartition(params.getPartitions());
    }
}
