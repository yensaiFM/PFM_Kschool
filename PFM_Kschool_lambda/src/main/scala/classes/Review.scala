package classes

class Review extends Serializable{
  var userid:Int = 0
  var movieid:Int = 0
  var rating:Double = 0.0d
  var timestamp:Long = 0


  override def toString():String =
  {
    return userid.toString + "|" + movieid.toString + "|" + rating.toString  + "|" + timestamp.toString
  }
}

case class ReviewFilm(userid:Int,
                      movieid:Int,
                      rating:Double,
                      timestamp:Long,
                      director:String,
                      genres:String,
                      title:String)

// 1. Films best rating by genre
case class ReviewFilmGenre(movieid:Int,
                           genres:String,
                           rating:Double,
                           title:String)

// 2. Films best rating by director
case class ReviewFilmDirector(movieid:Int,
                              director:String,
                              rating:Double,
                              title:String)

// 3. Number visualizations films (we consider one review = one view)
case class FilmView(movieid:Int,
                    total:Int,
                    title:String)

// 4. Genre best review
case class GenreReview(genres:String,
                       rating:Double)

// 5. Director best review
case class DirectorReview(director:String,
                          rating:Double)

// 6. Day of week more view films
case class DayView(day:String,
                   month:String,
                   year:Int,
                   total:Int)

// 7. Hours that are more consume
case class HoraryView(day:Int,
                      month:Int,
                      year:Int,
                      hour:Int,
                      total:Int)

// 8. Average films view by year/film, month/film, day/film
case class YearViewCount(year:Int,
                         movieid:Int,
                         title:String,
                         total:Long)

case class YearViewTotal(year:Int,
                         total:Long)

case class YearViewAvg(year:Int,
                       movieid:Int,
                       title:String,
                       avg:Float)

case class MonthViewCount(year:Int,
                          month:Int,
                          movieid:Int,
                          title:String,
                          total:Long)

case class MonthViewTotal(year:Int,
                          month:Int,
                          total:Long)

case class MonthViewAvg(year:Int,
                        month:Int,
                        movieid:Int,
                        title:String,
                        avg:Float)

case class DayViewCount(year:Int,
                        month:Int,
                        day:Int,
                        movieid:Int,
                        title:String,
                        total:Long)

case class DayViewTotal(year:Int,
                        month:Int,
                        day:Int,
                        total:Long)

case class DayViewAvg(year:Int,
                      month:Int,
                      day:Int,
                      movieid:Int,
                      title:String,
                      avg:Float)