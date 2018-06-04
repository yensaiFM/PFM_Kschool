package classes

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class TweetParser {
  def parse(row:String): Tweet =
  {
    var parsed = new Tweet();
    // Here we must convert json to save as object
    //println("Init parse")
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    var tweet = mapper.readValue(row, classOf[Tweet])
    //println(s"row: " + row)
    parsed.id = tweet.id
    parsed.text = tweet.text
    parsed.user = tweet.user
    parsed.timestamp = tweet.timestamp
    parsed.lang = tweet.lang
    parsed.hastag = tweet.hastag
    return parsed;
  }
}
