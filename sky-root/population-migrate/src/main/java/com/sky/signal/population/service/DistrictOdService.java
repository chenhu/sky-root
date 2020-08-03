package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.CellLoader;
import com.sky.signal.population.processor.OdProcess;
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
    private transient OdProcess odProcess;

    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient TraceProcessor traceProcessor;

    @Override
    public void compute() {
        //基站信息合并到全省信令数据
        final Broadcast<Map<String, Row>> provinceCell = cellLoader.loadCell();
        //加载预处理后的OD数据
        DataFrame odDf = odProcess.loadOd();
        //合并区县信息到OD数据
        odDf = odProcess.mergeOdWithCell(provinceCell, odDf).persist(StorageLevel.MEMORY_AND_DISK());
        //找出目标区域内满足OD条件的手机号码，并用这些号码找出其在全省其他地区的OD

        FileUtil.saveFile(odDf, FileUtil.FileType.CSV, params.getProvinceODFilePath());

        //最后释放持久化的数据
        odDf.unpersist();

    }
}
