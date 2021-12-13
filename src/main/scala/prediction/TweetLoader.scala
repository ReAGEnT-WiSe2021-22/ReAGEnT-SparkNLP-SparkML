package prediction

import org.apache.spark.rdd.RDD
import org.bson.Document

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import utils.SentimentAnalysisUtils


/**
 * Klasse zum L
 */
object TweetLoader {
  val dtf_new: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")

  /**
   * Transforms a string-timestamp from Twitter into a LocalDateTime object
   * @param date Date field of tweet
   * @return LocalDateTime object
   */
  def transformTwitterDate(date:String):LocalDateTime = LocalDateTime.parse(date.splitAt(19)._1, dtf_new)

  /**
   * @param document Tweet
   * @return String with tweet text
   */
  def getText(document:Document):String = document.getString("tweet")

  /**
   * @param document Tweet
   * @return String with party, that was mentioned in the tweet
   */
  def getParty(document:Document):String = document.getString("partei")

  /**
   * @param document Tweet
   * @return Date when tweet was created
   */
  def getDate(document:Document):LocalDateTime = transformTwitterDate(document.getString("created_at"))

  /**
   * @param rdd RDD with loaded tweets
   * @return RDD with filtered attributes of the tweet (party, text, created_at) and the sentiment value
   */
  def prepareTweets(rdd:RDD[Document]):RDD[TrainingTweet] = {
    rdd.map( x => {
      val text = getText(x)
      val sentiment = SentimentAnalysisUtils.detectSentiment(text)

      new TrainingTweet(getParty(x), text, getDate(x), sentiment)
    } )
  }

}
