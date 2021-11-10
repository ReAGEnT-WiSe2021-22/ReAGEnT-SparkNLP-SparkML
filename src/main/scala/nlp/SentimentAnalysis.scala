package nlp

import domain.Tweet
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object SentimentAnalysis {

  def getTFIDFScore(data: RDD[Tweet]):RDD[Vector] = {
    val tweetTexts = data.map(_.cleanedText)
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(tweetTexts)
    tf.cache

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)

    tfidf
  }

//  def getWordEmbeddings(data: Unit):Unit = {
//    ???
//  }
//
//  def getSentiment(data: RDD[Tweet]):RDD[Tweet, Double] = {
//    ???
//  }
}
