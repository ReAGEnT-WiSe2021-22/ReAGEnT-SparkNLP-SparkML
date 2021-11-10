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
                  cleanedText:List[String] = Nil
                )
