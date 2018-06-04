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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._
import com.datastax.spark
import com.datastax.spark._
import com.datastax.spark.connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnector._


import scala.util.Try

object StreamingFilmJob{
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext("PMP Kschool Spark")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "localhost:9092",
      "group.id" -> "com.pfm.kschool",
      "auto.offset.reset" -> "largest"
    )

    val topicsSet = "film".split(",").toSet
    val filmDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    var inputStream = filmDirectStream.map(_._2);
    inputStream = inputStream.repartition(2);

    // Save data at HDFS in directory RAW
    inputStream.saveAsTextFiles("hdfs://localhost:9000/pfm/data/film/")
    // Parse string value as film object
    var parsedStream = inputStream.mapPartitions{
      rows =>
        val parser = new FilmParser();
        rows.map { row => parser.parse(row)}
    }

    // Save to Cassandra
    parsedStream.foreachRDD( rdd => {
      if (! rdd.isEmpty()) {
        rdd.map( film => {
          (film.movieid, film.imdbid, film.title, film.adult, film.genres, film.originalLanguage, film.productionCompanies,
          film.releaseDate, film.runtime, film.status, film.director)
        }). saveToCassandra("pfm", "film", SomeColumns(
          "movieid", "imdbid", "title", "adult", "genres", "originallanguage", "productioncompanies", "releasedate",
          "runtime", "status", "director")
        )
      } else {
        println("No records to save film")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
