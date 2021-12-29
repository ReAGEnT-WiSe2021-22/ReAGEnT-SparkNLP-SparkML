package prediction

import org.apache.spark.rdd.RDD
import org.bson.Document

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import utils.SentimentAnalysisUtils

import java.sql.Date
import scala.util.Random


/**
 * Class that provides methods to filter needed attributes from tweets and create a RDD with TrainingTweet-objects
 */
object TweetLoader {
  val dtf:DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd")

  val sentimentUtil = new SentimentAnalysisUtils()

  /**
   * Transforms a string-timestamp from Twitter into a LocalDateTime object
   * @param date Date field of tweet
   * @return LocalDateTime object
   */
  def transformTwitterDate(date:String):LocalDate = LocalDate.parse(date.take(19), dtf)

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
  def getDate(document:Document):Date = {
    val localDate = transformTwitterDate(document.getString("created_at"))
    Date.valueOf(localDate)
  }

  /**
   * @param text Unchanged tweet text
   * @return text without stopwords
   */
  def removeStopWords(text:String):String = {
    //TODO Will be done maybe later
    ???
  }



  /**
   * @param rdd RDD with loaded tweets
   * @return RDD with filtered attributes of the tweet (party, text, created_at) and the sentiment value
   */
  def prepareTweets(rdd:RDD[Document]):RDD[TrainingTweet] = {
    rdd.map( x => {
      val text = getText(x)
      //val sentiment = sentimentUtil.detectSentiment(text)
      val randgen= new Random()
      val sentiment = randgen.nextDouble()*5.0 //TODO random instead of sentiments (for now)

      TrainingTweet(getParty(x), text, getDate(x), sentiment)
    } )
  }

}
