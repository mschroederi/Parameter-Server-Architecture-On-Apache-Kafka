package de.hpi.datastreams.ml;

import lombok.Getter;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Metrics {

    @Getter
    private double f1;
    @Getter
    private double accuracy;

    static Metrics from(Dataset<Row> predictionAndLabels) {
        predictionAndLabels.cache();
        Metrics metrics = new Metrics();

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        metrics.f1 = evaluator.setMetricName("f1").evaluate(predictionAndLabels);
        metrics.accuracy = evaluator.setMetricName("accuracy").evaluate(predictionAndLabels);

        return metrics;
    }
}
