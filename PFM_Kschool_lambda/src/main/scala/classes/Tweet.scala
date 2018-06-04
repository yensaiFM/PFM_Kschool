package classes

class Tweet extends Serializable{
  var id:String = null
  var text:String = null
  var user:String = null
  var timestamp:Long = 0
  var lang:String = null
  var hastag:String = null


  override def toString():String = {
    return id + "|" + text + "|" + user + "|" + timestamp.toString + "|" + lang + "|" + hastag
  }
}

case class TweetSentiment(id:String,
                          text:String,
                          user:String,
                          timestamp:Long,
                          lang:String,
                          hastag:String,
                          sentiment:String)

case class TweetBySentiment(hastag:String,
                            sentiment:String,
                            total:Int)
