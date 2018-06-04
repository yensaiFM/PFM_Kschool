package streaming

import classes._
import com.datastax.spark.connector._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import utils.Utils._
//import utils.SentimentalAnalysis._

import scala.io.Source

object StreamingTweetJob{

  val positiveWords = Source.fromFile("dataset/positive-words.txt").getLines().toList
  val negativeWords = Source.fromFile("dataset/negative-words.txt").getLines().toList

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

    val topicsSet = "tweet".split(",").toSet
    val reviewDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    var inputStream = reviewDirectStream.map(_._2);
    inputStream = inputStream.repartition(2);

    // Save data at HDFS in directory RAW
    inputStream.saveAsTextFiles("hdfs://localhost:9000/pfm/data/tweet/")
    // Parse string value as review object
    var parsedStream = inputStream.mapPartitions{
      rows =>
        val parser = new TweetParser();
        rows.map { row => parser.parse(row)}
    }

    // Save to Cassandra
    parsedStream.foreachRDD( rdd => {
      if (! rdd.isEmpty()) {
        val transform = rdd.map( tweet => {
          // We must calculate sentimental analysis tweet
          var sentiment = getTweetSentiment(tweet.text)
          new TweetSentiment(tweet.id, tweet.text, tweet.user, tweet.timestamp, tweet.lang, tweet.hastag, sentiment)
        })
        // Save to Cassandra
        transform.saveToCassandra("pfm", "tweet", SomeColumns("id", "text", "user", "timestamp", "lang", "hastag", "sentiment"))

        // Save to HDFS
        //transform.saveAsTextFile("hdfs://localhost:9000/pfm/etl/tweet/tweetsentiment_")
        val tweetsentimentDF = transform.toDF().selectExpr("id", "text", "user", "timestamp", "lang", "hastag", "sentiment")
        tweetsentimentDF
          .write
          .mode(SaveMode.Append)
          .parquet("hdfs://localhost:9000/pfm/etl/tweet/tweetsentiment_")

        // Obtain KPIs:
        // 1. Sentimental analysis group by hastag
        val tweetBySentiment = transform
          .map(tweet => ((tweet.hastag, tweet.sentiment), 1))
          .reduceByKey(_+_)

        val tweetBySentiment_transform = tweetBySentiment.map{ row =>
          new TweetBySentiment(row._1._1, row._1._2, row._2)
        }
        // Save to Cassandra. We must obtain total if row exists in table to update value or add new row
        val joinTweetBySentiment = tweetBySentiment_transform.leftJoinWithCassandraTable("pfm", "stream_view_hastag_by_sentimental")
          .select("total")
          .on(SomeColumns("hastag", "sentiment"))
        //println(s" count:" + joinTweetBySentiment.count())
        //joinTweetBySentiment.take(10).foreach(println)
        val filmByGenreFinal_transform = joinTweetBySentiment.map { row =>
          var total = row._1.total
          if(row._2.isDefined){
            total = total + row._2.get.getInt("total")
          }
          new TweetBySentiment(row._1.hastag, row._1.sentiment, total)
        }
        //filmByGenreFinal_transform.take(3).foreach(println)
        filmByGenreFinal_transform.saveToCassandra("pfm","stream_view_hastag_by_sentimental", SomeColumns("hastag", "sentiment", "total"))

      } else {
        println("No records to save")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def getTweetSentiment(tweet:String):String = {
    var count_positive = 0
    var count_negative = 0
    //println(s" text:" + tweet)
    for(positiveW <- positiveWords){
      if(tweet.contains(positiveW)){
        //println(s" post: " + positiveW)
        count_positive = count_positive + 1
      }
    }
    for(negativeW <- negativeWords){
      if(tweet.contains(negativeW)){
        //println(s" negat: " + negativeW)
        count_negative = count_negative + 1
      }
    }
    if(count_positive > count_negative){
      return "positive"
    }else if (count_negative > 0){
      return "negative"
    }else{
      return "neutral"
    }
  }
}
