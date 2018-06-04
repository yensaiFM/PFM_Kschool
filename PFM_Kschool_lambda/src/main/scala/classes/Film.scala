package classes

class Film extends Serializable{
  var movieid:Int = 0
  var imdbid:String = null
  var title:String = null
  var adult:Boolean = false
  var genres:String = null
  var originalLanguage:String = null
  var productionCompanies:String = null
  var releaseDate:String = null
  var runtime:Int = 0
  var status:String = null
  var director:String = null

  override def toString():String = {
    return movieid.toString + "|" + imdbid + "|" + title + "|" + adult.toString + "|" + genres + "|" + originalLanguage + "|" + productionCompanies + "|" + releaseDate + "|" + runtime.toString + "|" + status + "|" + director
  }
}
