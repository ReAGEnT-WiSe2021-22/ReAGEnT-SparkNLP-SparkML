package utils

import domain.Tweet
import nlp.SentimentAnalysis
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime}
import java.util.Locale
import scala.collection.JavaConverters._

object TwitterUtils {

  def applySentimentToTweets(tweets: RDD[Tweet]): RDD[Tweet]= {

    val tweetsWithSentiment = tweets.map(tweet =>
      Tweet(
        tweet.tweetID,
        tweet.createdAt,
        tweet.userID,
        tweet.userName,
        tweet.name,
        tweet.party,
        tweet.hashtags,
        tweet.text,
        tweet.cleanText,
        SentimentAnalysis.detectSentiment(tweet.cleanText.mkString)
      )
    )

    tweetsWithSentiment
  }

  def parseDocumentToTweet(rdd: RDD[Document]): RDD[Tweet] = {
    rdd.map(entry => {
      val text = entry.get("tweet").asInstanceOf[String]
      Tweet(
        getLong(entry.get("id")).get,
        getTwitterDate(entry.get("created_at").asInstanceOf[String]),
        getLong(entry.get("user_id")).get,
        entry.get("username").asInstanceOf[String],
        entry.get("name").asInstanceOf[String],
        entry.get("partei").asInstanceOf[String],
        entry.get("hashtags").asInstanceOf[java.util.List[String]].asScala.toList,
        text,
        Nil,
        0.0 //NOT UNDERSTOOD value for sentiment analysis
      )
    })
  }

  def getLong(number: Any): Option[Long] = number match {
    case i: Int => Some(i.toLong)
    case l: Long => Some(l)
    case s: String => Some(s.toLong)
    case _ => None
  }

  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss X", Locale.ENGLISH)

  def getTwitterDate(date: String):Timestamp={

    val (ts,zone)= date.splitAt(20)
    val timestamp=ts+{if (zone=="CET") "+0100" else "+0200"}

    try {
      val odt= OffsetDateTime.parse(timestamp, dtf)
      val i:Instant= odt.toInstant
      Timestamp.from(i)

    } catch {
      case e: Exception =>
        println(e.getMessage)
        println(s"Invalid Date format ${timestamp}")
        Timestamp.from(Instant.now)
    }
  }
}