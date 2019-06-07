package com.lesits.ml.processor.gaode;

import com.lesits.ml.processor.Processor;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/27 14:43
 * description: 根据高德路况历史数据，用朴素贝叶斯模型训练，生成通过道路id、时间分类、日期分类三个特性来确定 道路拥堵等级 的值
 */
@Service
public class TrainData implements Processor, Serializable {
    @Autowired
    private transient SQLContext sqlContext;

    @Override
    public void process() {
        // Load training data
        Dataset<Row> dataFrame = sqlContext.read().format("libsvm").load("/Users/chenhu/data/road-status/save/road-libsvm/*");
        // Split the data into train and test
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        // create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();

        // train the model
        NaiveBayesModel model = nb.fit(train);
//        try {
//            model1.write().overwrite().save("/Users/chenhu/data/road-status/NaiveBayesModel.m");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        NaiveBayesModel model = NaiveBayesModel.load("/Users/chenhu/data/road-status/NaiveBayesModel.m");
        // Select example rows to display.
        Dataset<Row> predictions = model.transform(test);
        predictions.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.getAs("features").toString());
            }
        });
        // compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);


    }
    private Tuple2<Integer, Integer> transformDate(String dateTimeString) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dateTime = formatter.parseDateTime(dateTimeString);
        Date day = dateTime.toDate();
        Integer dayInt = getDateType(day.toString());
        DateTime time = dateTime.toDateTime();
        return new Tuple2<>(dayInt, time.getSecondOfDay());
    }

    private Integer getDateType(String date) {
        return 0;
    }
}
