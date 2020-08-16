package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.CellLoader;
import com.sky.signal.population.processor.OdProcess;
import com.sky.signal.population.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
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
    @Override
    public void compute() {
        //基站信息合并到全省信令数据
        final Broadcast<Map<String, Row>> provinceCell = cellLoader.loadCell();
        //按天处理
        for(String path: params.getProvinceODFilePaths(params.getDistrictCode().toString())) {
            //加载预处理后的OD数据
            DataFrame odDf = odProcess.loadOd(path);
            //合并区县信息到OD数据
            odDf = odProcess.mergeOdWithCell(provinceCell, odDf);
            //合并同区县的OD并找出符合条件的OD记录
            odDf = odProcess.provinceResultOd(odDf).cache();
            String date = path.substring(path.length() - 8);
            FileUtil.saveFile(odDf, FileUtil.FileType.CSV, params.getDestDistrictOdFilePath(params.getDistrictCode().toString(),date));
            //生成区县停留时间有要求的区县出行OD
            odDf = odProcess.provinceDurationLimitedOd(odDf);
            FileUtil.saveFile(odDf, FileUtil.FileType.CSV, params.getLimitedDestDistrictOdFilePath(params.getDistrictCode().toString(),date));
        }
    }
}
