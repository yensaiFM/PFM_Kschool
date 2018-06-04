package utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkContext, SparkConf}

object Utils {

  def getSparkContext(appName: String) = {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.cassandra.connection.host", "127.0.0.1") // IP externa
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .setMaster("local[*]") // Not if in production

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir("hdfs://localhost:9000/pfm/checkpoint")
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

}
