package utils

import org.apache.spark.rdd.RDD
import org.bson.Document
import training.TrainingTweet

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Class that provides methods to filter needed attributes from tweets and create a RDD with TrainingTweet-objects
 *
 * @author Schander 572893
 */
object TweetLoader {
  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  /**
   * Transforms a string-timestamp from Twitter into a LocalDateTime object
   *
   * @param date Date field of tweet
   * @return LocalDate object
   */
  def transformTwitterDate(date: String): LocalDate = LocalDate.parse(date.take(10), dtf)

  /**
   * @param document Tweet
   * @return String with tweet text
   */
  def getText(document: Document): String = document.getString("tweet")

  /**
   * @param document Tweet
   * @return String with party, that was mentioned in the tweet
   */
  def getParty(document: Document): String = document.getString("partei")

  /**
   * @param document Tweet
   * @return Date when tweet was created
   */
  def getDate(document: Document): Date = {
    val localDate = transformTwitterDate(document.getString("created_at"))
    Date.valueOf(localDate)
  }


  /**
   * @param rdd RDD with loaded tweets from DB
   * @return RDD with filtered attributes of the tweet (party, text, created_at) and the sentiment value
   */
  def prepareTweets(rdd: RDD[Document]): RDD[TrainingTweet] = {
    rdd.map(x => {
      val sentiment = SentimentAnalysisUtils.detectSentiment(getText(x))
      TrainingTweet(getParty(x), getDate(x), sentiment)
    })
  }
}
