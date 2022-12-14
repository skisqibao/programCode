package com.knowyou.model


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.DataFrame

object GbdtModel {
    def GbdtTrain(data:DataFrame): Unit = {

      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(data)

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(data)

      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      val gbt = new GBTClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")
        .setMaxIter(10)

      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      predictions.select("predictedLabel", "label", "features").show(50)

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      println("acc = " + accuracy)

      val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
      println("Learned classification GBT model:\n" + gbtModel.toDebugString)
    }
}
