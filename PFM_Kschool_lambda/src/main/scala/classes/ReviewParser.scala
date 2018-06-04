package classes

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature


class ReviewParser {
  def parse(row:String): Review =
  {
    var parsed = new Review();
    // Here we must convert json to save as object
    //println("Init parse")
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    val review = mapper.readValue(row, classOf[Review])
    println(s"row: " + row)
    //println(s"userid: " + review.userid)
    //println(s"movieid: " + review.movieid)
    //println(s"rating: " + review.rating)
    //println(s"created: " + review.timestamp)
    parsed.userid = review.userid
    parsed.movieid = review.movieid
    parsed.rating = review.rating
    parsed.timestamp = review.timestamp
    //parsed.director = review.director
    //parsed.genres = review.genres
    return parsed;
  }
}
