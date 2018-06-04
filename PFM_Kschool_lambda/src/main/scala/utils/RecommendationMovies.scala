package utils

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.DataFrame

object RecommendationMovies {

  def train_recommendation(reviews: DataFrame) = {
    val Array(training, test) = reviews.randomSplit(Array(0.8, 0.2))
    // Create the recommendation model using ALS
    val als = new ALS()
      .setRank(10) // the number of latent factors in the model
      .setMaxIter(10) // the maximum number of iterators
      .setRegParam(0.01) // the regularization parameter
      .setUserCol("userid")
      .setItemCol("movieid")
      .setRatingCol("rating")
    val model = als.fit(training)

    // With cold start strategy to 'drop' ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")

    // Evaluate the performance of the model by comparing the predictive values wiht the original values
    val predictions = model.transform(test)

    // Metric RMSE obtain the average of the squares of the errors between what is estimated and the existing data
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    // The lower the mean squared error value, the more accurate the model
    println(s"Root-mean-square error = $rmse")  // We obtain 0.6562

    // Save model to use later
    model.save("hdfs://localhost:9000/pfm/model/recommendationALS.model")
    //model
  }


}
