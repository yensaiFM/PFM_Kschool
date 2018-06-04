package streaming

import java.util.HashMap

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, TaskContext}
import utils.Utils._
import functions._
import classes._
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, cassandra}
import org.apache.spark.sql.cassandra._
import com.datastax.spark
import com.datastax.spark._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming._
import org.joda.time.DateTime

import scala.util.Try

object StreamingReviewJob{
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext("PMP Kschool Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "localhost:9092",
      "group.id" -> "com.pfm.kschool",
      "auto.offset.reset" -> "largest"
    )

    val topicsSet = "review".split(",").toSet
    val reviewDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    var inputStream = reviewDirectStream.map(_._2);
    inputStream = inputStream.repartition(2);

    // Save data at HDFS in directory RAW
    inputStream.saveAsTextFiles("hdfs://localhost:9000/pfm/data/review/")
    // Parse string value as review object
    var parsedStream = inputStream.mapPartitions{
      rows =>
        val parser = new ReviewParser();
        rows.map { row => parser.parse(row)}
    }

    parsedStream.foreachRDD{ rdd => {
      val internalJoin = rdd.joinWithCassandraTable("pfm", "film")
        .select("director", "genres", "title")
        .on(SomeColumns("movieid"))
      val transform = internalJoin.map { row => {
        var director = "undefined"
        var genres = "undefined"
        var title = "undefined"
        if(!row._2.get[Option[String]]("director").isEmpty){
          director = row._2.getString("director")
        }
        if(!row._2.get[Option[String]]("genres").isEmpty){
          genres = row._2.getString("genres")
        }
        if(!row._2.get[Option[String]]("title").isEmpty){
          title = row._2.getString("title")
        }
        new ReviewFilm(row._1.userid, row._1.movieid, row._1.rating, row._1.timestamp, director, genres, title)
      }}
      // Save to Cassandra
      transform.saveToCassandra("pfm","reviewfilm", SomeColumns("userid", "movieid", "rating", "timestamp", "director", "genres", "title"))
      // Save to HDFS
      //transform.saveAsTextFile("hdfs://localhost:9000/pfm/etl/review/reviewfilm_")
      val reviewfilmDF = transform.toDF().selectExpr("userid", "movieid", "rating", "timestamp", "director", "genres", "title")
      reviewfilmDF
        .write
        .mode(SaveMode.Append)
        .parquet("hdfs://localhost:9000/pfm/etl/review/reviewfilm_")

      // Obtain different KPIs:
      // 1. Films best rating by genre
      val filmByGenre = transform
        .map(reviewfilm => ((reviewfilm.movieid, reviewfilm.genres), (reviewfilm.rating, reviewfilm.title)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2))

      val filmByGenre_transform = filmByGenre.map{ row =>
        new ReviewFilmGenre(row._1._1, row._1._2, row._2._1, row._2._2)
      }
      // Save to Cassandra. We must obtain rating if row exists in table to update value or add new row
      val joinFilmByGenre = filmByGenre_transform.leftJoinWithCassandraTable("pfm", "stream_view_best_film_by_genre")
        .select("rating")
        .on(SomeColumns("movieid", "genres"))
      //println(s" count:" + joinFilmByGenre.count())
      //joinFilmByGenre.take(10).foreach(println)
      val filmByGenreFinal_transform = joinFilmByGenre.map { row =>
        var rating = row._1.rating
        if(row._2.isDefined){
          rating = rating + row._2.get.getDouble("rating")
        }
        new ReviewFilmGenre(row._1.movieid, row._1.genres, rating, row._1.title)
      }
      //filmByGenreFinal_transform.take(3).foreach(println)
      filmByGenreFinal_transform.saveToCassandra("pfm","stream_view_best_film_by_genre", SomeColumns("movieid", "genres", "rating", "title"))


      // 2. Films best rating by director
      val filmByDirector = transform
        .map(reviewfilm => ((reviewfilm.movieid, reviewfilm.director), (reviewfilm.rating, reviewfilm.title)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2))
      val filmByDirector_transform = filmByDirector.map{ row =>
        new ReviewFilmDirector(row._1._1, row._1._2, row._2._1, row._2._2)
      }
      // Save to Cassandra. We must obtain rating if row exists in table to update value or add new row
      val joinFilmByDirector = filmByDirector_transform.leftJoinWithCassandraTable("pfm","stream_view_best_film_by_director")
        .select("rating")
        .on(SomeColumns("movieid", "director"))
      //println(s" count:" + joinFilmByDirector.count())
      //joinFilmByDirector.take(10).foreach(println)
      val filmByDirectorFinal_transform = joinFilmByDirector.map { row =>
        var rating = row._1.rating
        if(row._2.isDefined){
          rating = rating + row._2.get.getDouble("rating")
        }
        new ReviewFilmDirector(row._1.movieid, row._1.director, rating, row._1.title)
      }
      filmByDirectorFinal_transform.saveToCassandra("pfm","stream_view_best_film_by_director", SomeColumns("movieid", "director", "rating", "title"))


      // 3. Number visualizations films (we consider one review = one view)
      val filmView = transform
        .map(reviewfilm => ((reviewfilm.movieid), (1, reviewfilm.title)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2))
      val filmView_transform = filmView.map{ row =>
        new FilmView(row._1, row._2._1, row._2._2)
      }
      // Save to Cassandra. We must obtain total if row exists in table to update value or add new row
      val joinFilmView = filmView_transform.leftJoinWithCassandraTable("pfm","stream_view_film_view")
        .select("total")
        .on(SomeColumns("movieid"))
      //println(s" count: " + joinFilmView.count())
      //joinFilmView.take(10).foreach(println)
      val filmViewFinal_transform = joinFilmView.map { row =>
        var total = row._1.total
        if(row._2.isDefined){
          total = total + row._2.get.getInt("total")
        }
        new FilmView(row._1.movieid, total, row._1.title)
      }
      filmViewFinal_transform.saveToCassandra("pfm","stream_view_film_view", SomeColumns("movieid", "total", "title"))

      // 4. Genre best review
      val genreReview = transform
        .map(reviewfilm => (reviewfilm.genres, reviewfilm.rating))
        .reduceByKey(_+_)
      val genreReview_transform = genreReview.map { row =>
        new GenreReview(row._1, row._2)
      }
      // Save to Cassandra. We must obtain rating if row exists in table to update value or add new row
      val joinGenreReview = genreReview_transform.leftJoinWithCassandraTable("pfm","stream_view_best_genre_review")
        .select("rating")
        .on(SomeColumns("genres"))
      //println(s" count: " + joinGenreReview.count())
      //joinGenreReview.take(10).foreach(println)
      val genreReviewFinal_transform = joinGenreReview.map { row =>
        var rating = row._1.rating
        if(row._2.isDefined){
          rating = rating + row._2.get.getDouble("rating")
        }
        new GenreReview(row._1.genres, rating)
      }
      genreReviewFinal_transform.saveToCassandra("pfm","stream_view_best_genre_review", SomeColumns("genres", "rating"))

      // 5. Director best review
      val directorReview = transform
        .map(reviewfilm => (reviewfilm.director, reviewfilm.rating))
        .reduceByKey(_+_)
      val directorReview_transform = directorReview.map { row =>
        new DirectorReview(row._1, row._2)
      }
      // Save to Cassandra. We must obtain rating if row exists in table to update value or add new row
      val joinDirectorReview = directorReview_transform.leftJoinWithCassandraTable("pfm","stream_view_best_director_review")
        .select("rating")
        .on(SomeColumns("director"))
      //println(s" count: " + joinDirectorReview.count())
      //joinDirectorReview.take(10).foreach(println)
      val directorReviewFinal_transform = joinDirectorReview.map { row =>
        var rating = row._1.rating
        if(row._2.isDefined){
          rating = rating + row._2.get.getDouble("rating")
        }
        new DirectorReview(row._1.director, rating)
      }
      directorReviewFinal_transform.saveToCassandra("pfm","stream_view_best_director_review", SomeColumns("director", "rating"))

      // 6. Day of week more view films
      val dayView = transform
        .map(reviewfilm => {
          var date = new DateTime(reviewfilm.timestamp)
          var dayOfWeek = date.dayOfWeek().getAsText()
          var month = date.monthOfYear().getAsString()
          var year = date.year().get()
          ((dayOfWeek, month, year), 1)
        })
        .reduceByKey(_+_)
      val dayView_transform = dayView.map { row =>
        new DayView(row._1._1, row._1._2, row._1._3, row._2)
      }
      // Save to Cassandra. We must obtain total if row exists in table to update value or add new row
      val joinDayView = dayView_transform.leftJoinWithCassandraTable("pfm","stream_view_best_day_week")
        .select("total")
        .on(SomeColumns("day", "month", "year"))
      //println(s" count: " + joinDayView.count())
      //joinDayView.take(10).foreach(println)
      val dayViewFinal_transform = joinDayView.map { row =>
        var total = row._1.total
        if(row._2.isDefined){
          total = total + row._2.get.getInt("total")
        }
        new DayView(row._1.day, row._1.month, row._1.year, total)
      }
      dayViewFinal_transform.saveToCassandra("pfm","stream_view_best_day_week", SomeColumns("day", "month", "year", "total"))

      // 7. Hours that are more consume
      val horaryView = transform
        .map(reviewfilm => {
          var date = new DateTime(reviewfilm.timestamp)
          var hour = date.hourOfDay().get()
          var day = date.dayOfMonth().get()
          var month = date.monthOfYear().get()
          var year = date.year().get()
          ((day, month, year, hour), 1)
        })
        .reduceByKey(_+_)
      val horaryView_transform = horaryView.map { row =>
        new HoraryView(row._1._1, row._1._2, row._1._3, row._1._4, row._2)
      }
      // Save to Cassandra. We must obtain total if row exists in table to update value or add new row
      val joinHoraryView = horaryView_transform.leftJoinWithCassandraTable("pfm", "stream_view_best_hour_view")
        .select("total")
        .on(SomeColumns("day", "month", "year", "hour"))
      println(s" count: " + joinHoraryView.count())
      joinHoraryView.take(10).foreach(println)
      val horaryViewFinal_transform = joinHoraryView.map { row =>
        var total = row._1.total
        if(row._2.isDefined){
          total = total + row._2.get.getInt("total")
        }
        new HoraryView(row._1.day, row._1.month, row._1.year, row._1.hour, total)
      }
      horaryViewFinal_transform.saveToCassandra("pfm", "stream_view_best_hour_view", SomeColumns("day", "month", "year", "hour", "total"))


      // 8. Average films view by year/film, month/film, day/film
      // It will do in batch job only
      /*
      val userVisitsYearAgg = transform
        .map(reviewfilm => {
          var date = new DateTime(reviewfilm.timestamp)
          var year = date.year().get()
          ((year, reviewfilm.movieid), (1, reviewfilm.title))
        }).reduceByKey((x, y) => (x._1 + y._1, x._2))
      val allVisitsYear = transform
        .map(reviewfilm => {
          var date = new DateTime(reviewfilm.timestamp)
          var year = date.year().get()
          (year, 1)
        }).reduceByKey(_+_)
      println(s" info: ")
      allVisitsYear.foreach(println)
      val userVisitsYearAgg_transform = userVisitsYearAgg.map{case((year, movieid), (sum, title)) => ((year), (movieid, title, sum))}
        .join(allVisitsYear)
        .map{case(year,((movieid, title, sum),totalsum)) =>
          (year, movieid, title, sum/totalsum.toFloat)
        }.collect()
      println(s"results: ")
      userVisitsYearAgg_transform.take(10).foreach(println)

      val userVisitsMonthAgg = transform
        .map(reviewfilm => {
          var date = new DateTime(reviewfilm.timestamp)
          var year = date.year().get()
          var month = date.monthOfYear().get()
          ((year, month, reviewfilm.movieid), (1, reviewfilm.title))
        }).reduceByKey((x, y) => (x._1 + y._1, x._2))
      val allVisitsMonth = transform
        .map(reviewfilm => {
          var date = new DateTime(reviewfilm.timestamp)
          var year = date.year().get()
          var month = date.monthOfYear().get()
          ((year, month), 1)
        }).reduceByKey(_+_)
      println(s" info month: ")
      allVisitsMonth.take(10).foreach(println)
      val userVisitsMonthAgg_transform = userVisitsMonthAgg.map{case((year, month, movieid), (sum, title)) => ((year, month), (movieid, title, sum))}
        .join(allVisitsMonth)
        .map{case((year, month),((movieid, title, sum),totalsum)) =>
          (year, month, movieid, title, sum/totalsum.toFloat)
        }.collect()
      println(s"results: ")
      userVisitsMonthAgg_transform.take(10).foreach(println)
      */
    }}
    ssc.start()
    ssc.awaitTermination()
  }

}
