package classes

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class FilmParser {
  def parse(row:String): Film =
  {
    var parsed = new Film();
    // Here we must convert json to save as object
    //println("Init parse")
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    var film = mapper.readValue(row, classOf[Film])
    println(s"row: " + row)
    parsed.movieid = film.movieid
    parsed.imdbid = film.imdbid
    parsed.title = film.title
    parsed.adult = film.adult
    parsed.genres = film.genres
    parsed.originalLanguage = film.originalLanguage
    parsed.productionCompanies = film.productionCompanies
    parsed.releaseDate = film.releaseDate
    parsed.runtime = film.runtime
    parsed.status = film.status
    parsed.director = film.director
    return parsed;
  }
}
