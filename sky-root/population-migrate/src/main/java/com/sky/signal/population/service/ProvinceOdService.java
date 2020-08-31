package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.CellLoader;
import com.sky.signal.population.processor.PointProcess;
import com.sky.signal.population.util.FileUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by Chenhu on 2020/7/11.
 * 全省OD分析服务
 * 需要分析出每个人在全省范围内的OD路径，每个点精确到区县
 */
@Service
public class ProvinceOdService implements ComputeService {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient PointProcess pointProcess;
    @Override
    public void compute() {
//基站信息合并到全省信令数据
        final Broadcast<Map<String, Row>> provinceCell = cellLoader.loadCell();
        //按天处理
        for(String path: params.getProvincePointFilePath()) {
            DataFrame odDf = pointProcess.loadPoint(path);
            //合并区县信息到OD数据
            odDf = pointProcess.mergePointWithCell(provinceCell, odDf).cache();
            //合并同区县的OD并找出符合条件的OD记录
            DataFrame noneLimitedDf = pointProcess.provinceNoneLimitedOd(odDf).cache();
            String date = path.substring(path.length() - 8);
            FileUtil.saveFile(noneLimitedDf, FileUtil.FileType.CSV, params.getDestProvinceOdFilePath(date));
            //生成区县停留时间有要求的区县出行OD
            DataFrame limitedDf = pointProcess.provinceLimitedOd(odDf);
            FileUtil.saveFile(limitedDf, FileUtil.FileType.CSV, params.getLimitedProvinceOdFilePath(date));
        }
    }
}
