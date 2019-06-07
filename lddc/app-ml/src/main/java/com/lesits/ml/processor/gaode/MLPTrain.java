package com.lesits.ml.processor.gaode;

import com.lesits.ml.conf.AppConf;
import com.lesits.ml.processor.Processor;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Serializable;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/27 14:43
 * description: 根据高德路况历史数据，利用MLP多层神经网络分类
 */
@Service("mlpTrain")
public class MLPTrain implements Processor, Serializable {
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient AppConf appConf;

    @Override
    public void process() {
        // 加载Libsvm格式数据用来做训练
        Dataset<Row> dataFrame = sqlContext.read().format("libsvm").load(appConf.getLibSvm()).repartition(appConf.getPartitions());
        // 训练样本分割为6、4， 60%训练，40% 做验证
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];
        // 两个隐藏层，神经元个数为 5 + 4， 参数个数为 3*5 + 5*4 + 4*6
        // 本来应该输出三个分类，但是会报一个indexOutOfBound 异常，查资料后，可以修改为比3大的，暂时定为5
        int[] layers = new int[] {3, 5, 4, 6};
        MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier().setLayers(layers)
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(50);
        // 开始训练
        MultilayerPerceptronClassificationModel model = trainer.fit(train);
        // 用测试集验证模型
        Dataset<Row> result = model.transform(test);
        // 检验正确率
        Dataset<Row> predictionAndLabels = result.select("prediction", "label");
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setMetricName("accuracy");
        Double accuracy = evaluator.evaluate(predictionAndLabels);
        System.out.println("Test set accuracy = " + accuracy);
        if(accuracy > 0.6d) {
            try {
                model.save(appConf.getModel());
            } catch (IOException e) {
                e.printStackTrace();

            }
        }
    }
}
