package com.sky.signal.population.processor;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.util.FileUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:21
 * description: 加载已经处理好的基站信息数据，并广播
 */
@Service("cellLoader")
public class CellLoader implements Serializable {
    @Autowired
    private JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;

    /**
     * 加载指定区县的基站数据
     * @param districtCode 区县编码
     * @return
     */
    public Broadcast<Map<String, Row>> loadCurrentDistrictCell(String districtCode) {
        DataFrame cellDf = FileUtil.readFile(FileUtil.FileType.CSV,
                CellSchemaProvider.CELL_SCHEMA, params.getCurrentDistrictCellPath(districtCode));
        Row[] cellRows = cellDf.filter(col("district").equalTo(districtCode)).collect();
        Map<String, Row> cellMap = new HashMap<>(cellRows.length);
        for (Row row : cellRows) {
            Integer tac = row.getInt(1);
            Long cell = row.getLong(2);
            cellMap.put(tac.toString() + '|' + cell.toString(), row);
        }
        final Broadcast<Map<String, Row>> cellVar = sparkContext.broadcast
                (cellMap);
        return cellVar;
    }
    /**
     * 加载基站数据
     * @return
     */
    public Broadcast<Map<String, Row>> loadCell() {
        DataFrame cellDf = FileUtil.readFile(FileUtil.FileType.CSV,
                CellSchemaProvider.CELL_SCHEMA, params.getCellFile());
        Row[] cellRows = cellDf.collect();
        Map<String, Row> cellMap = new HashMap<>(cellRows.length);
        for (Row row : cellRows) {
            Integer tac = row.getInt(1);
            Long cell = row.getLong(2);
            cellMap.put(tac.toString() + '|' + cell.toString(), row);
        }
        final Broadcast<Map<String, Row>> cellVar = sparkContext.broadcast
                (cellMap);
        return cellVar;
    }

}
