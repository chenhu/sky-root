package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.CellLoader;
import com.sky.signal.population.processor.TraceProcessor;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by Chenhu on 2020/7/11.
 * 区县OD分析服务
 * 分析出从当前区县到外部区县的OD以及从外部区县到当前区县的OD
 */
@Service
public class DistrictOdService implements ComputeService {
    @Autowired
    private transient ParamProperties params;

    //在单个区域逗留时间阀值为120分钟
    private static final int DISTRICT_STAY_MINUTE = 2 * 60;
    @Autowired
    private transient CellLoader cellLoader;

    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient TraceProcessor traceProcessor;

    @Override
    public void compute() {
        //当前要处理的区县编码
        String districtCode = params.getDistrictCode();
        //加载基站文件，并筛选出属于当前区县的基站
        final Broadcast<Map<String, Row>> currentDistrictCell = cellLoader
                .loadCurrentDistrictCell(districtCode);

        //加载全省指定日期的信令数据
        List<String> traceFilePathList = params.getProvinceTraceFilePath();
        DataFrame traceDf = traceProcessor.loadTrace(traceFilePathList)
                .persist(StorageLevel.DISK_ONLY());

        //根据基站内容筛选出属于当前区县的信令数据
        DataFrame currentDistrictDf = traceProcessor
                .filterCurrentDistrictTrace(currentDistrictCell, traceDf);

        //只保存停留时间大于等于2小时的手机号码信息
        DataFrame mergedDf = traceProcessor.getDistrictDf(currentDistrictDf,
                currentDistrictCell);

        //


    }
}
