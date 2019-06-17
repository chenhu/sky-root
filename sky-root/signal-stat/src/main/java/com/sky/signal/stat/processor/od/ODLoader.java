package com.sky.signal.stat.processor.od;

import com.sky.signal.stat.config.ParamProperties;
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
    public DataFrame loadOD(List<String> odFiles) {
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

    public DataFrame loadODTrace(List<String> odTraceFiles) {
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

    public DataFrame loadODTripStat(List<String> odTripFiles) {
        DataFrame odTripDf = null ;
        for (String odTripFile: odTripFiles) {
            if(odTripDf == null) {
                odTripDf = FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_TRIP_STAT_SCHEMA1, odTripFile);
            } else {
                odTripDf = odTripDf.unionAll(FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_TRIP_STAT_SCHEMA1, odTripFile));
            }
        }
        return odTripDf.repartition(params.getPartitions());
    }
}
