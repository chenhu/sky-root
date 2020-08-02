package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.CellLoader;
import com.sky.signal.population.processor.TraceProcessor;
import com.sky.signal.population.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Chenhu on 2020/7/11.
 * <pre>
 * 区县OD分析服务
 * 数据源为：普通OD分析后的结果
 * 分析出从当前区县到外部区县的OD以及从外部区县到当前区县的OD
 * 也就是找出在当前区县出现的手机号而且停留时间满足一定阀值的手机,
 * 找出这些手机在全省范围内的轨迹，然后进行省级范围内以区县为单位进行OD分析
 * </pre>
 */
@Service
@Slf4j
public class DistrictOdService implements ComputeService, Serializable {
    @Autowired
    private transient ParamProperties params;

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
        //加载全省指定日期的信令数据
        DataFrame traceDf = traceProcessor.loadTrace(params.getProvinceTraceFilePath());
        //基站信息合并到全省信令数据
        final Broadcast<Map<String, Row>> provinceCell = cellLoader.loadCell();
        DataFrame provinceSignalDf = traceProcessor.mergeCellSignal(traceDf, provinceCell).persist(StorageLevel.DISK_ONLY());
        //根据基站内容筛选出属于当前区县的信令数据
        DataFrame currentDistrictDf = traceProcessor.filterCurrentDistrictTrace(districtCode, provinceSignalDf);

        //只保存停留时间大于等于2小时的手机号码信息
        currentDistrictDf = traceProcessor.filterStayMsisdnTrace(currentDistrictDf).persist(StorageLevel.DISK_ONLY());

        //找出符合条件的号码指定日期内在全省的轨迹数据
        DataFrame provinceMsisdnDf = traceProcessor.filterSignalByMsisdn(currentDistrictDf, provinceSignalDf);
        //对指定日期符合条件的号码进行全省范围内OD分析，并生成OD出行轨迹
        DataFrame odDf = traceProcessor.provinceOd(provinceMsisdnDf);

        FileUtil.saveFile(odDf, FileUtil.FileType.CSV, params.getProvinceODFilePath());

        currentDistrictDf.unpersist();
        provinceSignalDf.unpersist();


    }
}
