package batch

import java.lang.management.ManagementFactory

import classes._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import utils.Utils._
import utils.RecommendationMovies._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import com.datastax.spark.connector._

class BatchJob(sc: SparkContext) extends Thread{
  var iteration = 0

  override def run() = {
    process()
    this.iteration = this.iteration + 1
    println("iteration: " + iteration.toString)
  }

  def getIteration():Int = {
    this.iteration
  }

  def addIteration() = {
    this.iteration = this.iteration + 1
  }
  def resetIteration() = {
    this.iteration = 0
    println("inicia a 0:" + this.iteration.toString)
  }

  def process() = {
    // Create spark context
    //val sc = getSparkContext("PMP Kschool Spark")
    val sqlContext = getSQLContext(this.sc)
    import sqlContext.implicits._


    // Obtain data RDD from ReviewFilm
    val reviewfilmDF = sqlContext.read.parquet("hdfs://localhost:9000/pfm/etl/review/reviewfilm_")
    reviewfilmDF.createOrReplaceTempView("reviewfilm")

    //reviewfilmDF.show(5)

    // 1. Films best rating by genre
    val filmByGenre = sqlContext.sql(
      """SELECT movieid, genres, sum(rating) as rating, title
         |FROM reviewfilm group by movieid, title, genres
        """.stripMargin)
    filmByGenre
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_best_film_by_genre"))
      .mode(SaveMode.Append)
      .save()

    // 2. Films best rating by director
    val filmByDirector = sqlContext.sql(
      """SELECT movieid, director, sum(rating) as rating, title
        |FROM reviewfilm GROUP BY movieid, title, director
      """.stripMargin)
    filmByDirector
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_best_film_by_director"))
      .mode(SaveMode.Append)
      .save()

    // 3. Number visualizations films (we consider one review = one view)
    val filmView = sqlContext.sql(
      """SELECT movieid, count(*) as total, title
        |FROM reviewfilm GROUP BY movieid, title
      """.stripMargin)
    filmView
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_film_view"))
      .mode(SaveMode.Append)
      .save()

    // 4. Genre best review
    val genreReview = sqlContext.sql(
      """SELECT genres, sum(rating) as rating
        |FROM reviewfilm GROUP BY genres
      """.stripMargin)
    genreReview
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_best_genre_review"))
      .mode(SaveMode.Append)
      .save()

    // 5. Director best review
    val directorReview = sqlContext.sql(
      """SELECT director, sum(rating) as rating
        |FROM reviewfilm GROUP BY director
      """.stripMargin)
    directorReview
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_best_director_review"))
      .mode(SaveMode.Append)
      .save()

    // 6. Day of week more view films
    val dayView = sqlContext.sql(
      """SELECT date_format(CAST(timestamp/1000 as timestamp), 'EEEE') as day, date_format(CAST(timestamp/1000 as timestamp), 'MMMM') as month,date_format(CAST(timestamp/1000 as timestamp), 'YYYY') as year, count(*) as total
        |FROM reviewfilm GROUP BY date_format(CAST(timestamp/1000 as timestamp), 'EEEE'), date_format(CAST(timestamp/1000 as timestamp), 'MMMM'), date_format(CAST(timestamp/1000 as timestamp), 'YYYY')
      """.stripMargin)
    dayView
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_best_day_week"))
      .mode(SaveMode.Append)
      .save()

    // 7. Hours that are more consume
    val horaryView = sqlContext.sql(
      """SELECT day(CAST(timestamp/1000 as timestamp)) as day, month(CAST(timestamp/1000 as timestamp)) as month, year(CAST(timestamp/1000 as timestamp)) as year, hour(CAST(timestamp/1000 as timestamp)) as hour, count(*) as total
        |FROM reviewfilm GROUP BY day(CAST(timestamp/1000 as timestamp)), month(CAST(timestamp/1000 as timestamp)), year(CAST(timestamp/1000 as timestamp)), hour(CAST(timestamp/1000 as timestamp))
      """.stripMargin)
    horaryView
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_best_hour_view"))
      .mode(SaveMode.Append)
      .save()

    // 8. Average films view by year/film, month/film, day/film
    // Average views films by year
    val yearViewCount = sqlContext.sql(
      """SELECT year(CAST(timestamp/1000 as timestamp)) as year, movieid, title, count(*) as total
        |FROM reviewfilm GROUP BY year(CAST(timestamp/1000 as timestamp)), movieid, title
      """.stripMargin)
      .map{ row => {
        var year = row.getAs[Int](0)
        var movieid = row.getAs[Int](1)
        YearViewCount(year, movieid, row.getAs[String](2), row.getAs[Long](3))}}
    val yearViewTotal = sqlContext.sql(
      """SELECT year(CAST(timestamp/1000 as timestamp)) as year, count(*) as total
        |FROM reviewfilm GROUP BY year(CAST(timestamp/1000 as timestamp))
      """.stripMargin)
      .map{ row => {
        var year = row.getAs[Int](0)
        YearViewTotal(year, row.getAs[Long](1))}}
    //yearViewCount.show(10)
    //yearViewTotal.show(10)
    val yearViewAvg_join = yearViewCount.join(yearViewTotal, "year")
      .map { row =>
        var avg_year = row.getAs[Long](3).toFloat / row.getAs[Long](4).toFloat
        //println (s"avg :" + avg_year)
        YearViewAvg(row.getAs[Int](0), row.getAs[Int](1), row.getAs[String](2), avg_year)
      }
    yearViewAvg_join
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_avg_view_film_by_year"))
      .mode(SaveMode.Append)
      .save()

    // Average views films by month
    val monthViewCount = sqlContext.sql(
      """SELECT year(CAST(timestamp/1000 as timestamp)) as year, month(CAST(timestamp/1000 as timestamp)) as month, movieid, title, count(*) as total
        |FROM reviewfilm GROUP BY year(CAST(timestamp/1000 as timestamp)), month(CAST(timestamp/1000 as timestamp)), movieid, title
      """.stripMargin)
      .map{ row => {
        var year = row.getAs[Int](0)
        var month = row.getAs[Int](1)
        var movieid = row.getAs[Int](2)
        MonthViewCount(year, month, movieid, row.getAs[String](3), row.getAs[Long](4))}}
    val monthViewTotal = sqlContext.sql(
      """SELECT year(CAST(timestamp/1000 as timestamp)) as year, month(CAST(timestamp/1000 as timestamp)) as month, count(*) as total
        |FROM reviewfilm GROUP BY year(CAST(timestamp/1000 as timestamp)), month(CAST(timestamp/1000 as timestamp))
      """.stripMargin)
      .map{ row => {
        var year = row.getAs[Int](0)
        var month = row.getAs[Int](1)
        MonthViewTotal(year, month, row.getAs[Long](2))}}
    //monthViewCount.show(10)
    //monthViewTotal.show(10)
    val monthViewAvg_join = monthViewCount.join(monthViewTotal, Seq("year", "month"))
      .map { row =>
        var avg_month = row.getAs[Long](4).toFloat / row.getAs[Long](5).toFloat
        //println (s"avg :" + avg_month)
        MonthViewAvg(row.getAs[Int](0), row.getAs[Int](1), row.getAs[Int](2), row.getAs[String](3), avg_month)
      }
    monthViewAvg_join
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_avg_view_film_by_month"))
      .mode(SaveMode.Append)
      .save()

    // Average views films by day
    val dayViewCount = sqlContext.sql(
      """SELECT year(CAST(timestamp/1000 as timestamp)) as year, month(CAST(timestamp/1000 as timestamp)) as month, day(CAST(timestamp/1000 as timestamp)) as day, movieid, title, count(*) as total
        |FROM reviewfilm GROUP BY year(CAST(timestamp/1000 as timestamp)), month(CAST(timestamp/1000 as timestamp)), day(CAST(timestamp/1000 as timestamp)), movieid, title
      """.stripMargin)
      .map{ row => {
        var year = row.getAs[Int](0)
        var month = row.getAs[Int](1)
        var day = row.getAs[Int](2)
        var movieid = row.getAs[Int](3)
        DayViewCount(year, month, day, movieid, row.getAs[String](4), row.getAs[Long](5))}}
    val dayViewTotal = sqlContext.sql(
      """SELECT year(CAST(timestamp/1000 as timestamp)) as year, month(CAST(timestamp/1000 as timestamp)) as month, day(CAST(timestamp/1000 as timestamp)) as day, count(*) as total
        |FROM reviewfilm GROUP BY year(CAST(timestamp/1000 as timestamp)), month(CAST(timestamp/1000 as timestamp)), day(CAST(timestamp/1000 as timestamp))
      """.stripMargin)
      .map{ row => {
        var year = row.getAs[Int](0)
        var month = row.getAs[Int](1)
        var day = row.getAs[Int](2)
        DayViewTotal(year, month, day, row.getAs[Long](3))}}
    //dayViewCount.show(10)
    //dayViewTotal.show(10)
    val dayViewAvg_join = dayViewCount.join(dayViewTotal, Seq("year", "month", "day"))
      .map { row =>
        var avg_day = row.getAs[Long](5).toFloat / row.getAs[Long](6).toFloat
        //println (s"avg :" + avg_day)
        DayViewAvg(row.getAs[Int](0), row.getAs[Int](1), row.getAs[Int](2), row.getAs[Int](3), row.getAs[String](4), avg_day)
      }
    dayViewAvg_join
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_avg_view_film_by_day"))
      .mode(SaveMode.Append)
      .save()


    // Obtain data RDD from TweetSentiment
    val tweetsentimentDF = sqlContext.read.parquet("hdfs://localhost:9000/pfm/etl/tweet/tweetsentiment_")
    tweetsentimentDF.createOrReplaceTempView("tweet")
    tweetsentimentDF.show(5)

    // 1. Sentimental analysis group by hastag
    val tweetBySentiment = sqlContext.sql(
      """SELECT hastag, sentiment, count(*) as total
        |FROM tweet GROUP BY hastag, sentiment
      """.stripMargin)
    tweetBySentiment
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_hastag_by_sentimental"))
      .mode(SaveMode.Append)
      .save()

    //train_recommendation(reviewfilmDF)
    // Load model
    val model = ALSModel.load("hdfs://localhost:9000/pfm/model/recommendationALS.model")

    // Generate top 10 movie recommendations for a specified set of users
    val users = reviewfilmDF.select("userid").distinct()
    val userFilmRecommendations = model.recommendForAllUsers(10)
      .flatMap((row: Row) => {
        val userID = row.getInt(0)
        val recommendations = row.getSeq[Row](1)
        recommendations.map {
          case Row(repoID: Int, score: Float) => {
            (userID, repoID, score)
          }
        }
      })
      .toDF("userid", "movieid", "prediction")
    userFilmRecommendations.show(4)
    userFilmRecommendations
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "pfm", "table" -> "batch_view_recommendation_films"))
      .mode(SaveMode.Append)
      .save()

    /*
    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    userRecs.show(3)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)
    movieRecs.show(3)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = reviewfilmDF.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    */
  }

  def truncateStreamViews(): Unit ={
    // Delete view pfm.stream_view_hastag_by_sentimental
    sc.cassandraTable("pfm", "stream_view_hastag_by_sentimental")
      .deleteFromCassandra("pfm", "stream_view_hastag_by_sentimental")

    // Delete view pfm.stream_view_best_fim_by_genre
    sc.cassandraTable("pfm", "stream_view_best_film_by_genre")
      .deleteFromCassandra("pfm", "stream_view_best_film_by_genre")

    // Delete view pfm.stream_view_best_film_by_director
    sc.cassandraTable("pfm", "stream_view_best_film_by_director")
      .deleteFromCassandra("pfm", "stream_view_best_film_by_director")

    // Delete view pfm.stream_view_film_view
    sc.cassandraTable("pfm", "stream_view_film_view")
      .deleteFromCassandra("pfm", "stream_view_film_view")

    // Delete view pfm.stream_view_best_genre_review
    sc.cassandraTable("pfm", "stream_view_best_genre_review")
      .deleteFromCassandra("pfm", "stream_view_best_genre_review")

    // Delete view pfm.stream_view_best_director_review
    sc.cassandraTable("pfm", "stream_view_best_director_review")
      .deleteFromCassandra("pfm", "stream_view_best_director_review")

    // Delete view pfm.stream_view_best_day_week
    sc.cassandraTable("pfm", "stream_view_best_day_week")
      .deleteFromCassandra("pfm", "stream_view_best_day_week")

    // Delete view pfm.stream_view_best_hour_view
    sc.cassandraTable("pfm", "stream_view_best_hour_view")
      .deleteFromCassandra("pfm", "stream_view_best_hour_view")
  }

  def joinBatchStreamViews(): Unit ={

  }

}