package domain

import java.sql.Timestamp

case class Tweet(
                  tweetID:Long,
                  createdAt:Timestamp,
                  userID: Long,
                  userName:String,
                  name:String,
                  party:String,
                  hashtags:List[String],
                  text:String,
                  cleanText:List[String],
                  var sentiment:Double
                ){
  def setSentiment(sentiment:Double) = {
    this.sentiment = sentiment
  }
}
