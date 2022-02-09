import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import domain.Tweet
import nlp.{PartyPrediction, Preprocessing, SentimentAnalysis}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils._

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("PartyPrediction")
      //.config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.tweets_bundestag_legislatur")
      //.config("spark.testing.memory", 2147480000)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    /*
      Loading the data

      if useLocalTweets is false: sparkSession needs connection to database on production VM
     */
    println("Loading Tweets.")

    var tweetDF:DataFrame = null
    val useLocalTweets = true
    val safeTweets = false
    val fileName:String = "tweets_raw_long"

    if (useLocalTweets) {
      val tweetRDD = IOUtils.RDDFromJsonFile[Tweet](fileName, true)
      tweetDF = tweetRDD.toDF().cache
    }
    else {
      val rdd = MongoSpark.load(sc).rdd
      tweetDF = TwitterUtils.parseDocumentToTweet(rdd).toDF().cache

      val tweetsPath = "src/main/resources/tweets_raw_long"
      if (safeTweets) IOUtils.DataFrameToJsonFile(tweetDF, tweetsPath, false)
    }


    //PREPROCESSING
    println("Preprocessing data.")

    val result = new Preprocessing(tweetDF).getResult()
    //transform preprocessing result into Tweets
    //taking less entries for testing purposes
    val cleanTweetsRDD = sc.parallelize(
      result.as[Tweet].rdd.cache.take(2000), numSlices = 10)
      .cache
    cleanTweetsRDD.take(5).foreach(f => println(f))


    //SENTIMENT ANALYSIS
    println("Calculate Sentiments")

//    val tweetsWithSentiment = TwitterUtils.applySentimentToTweets(cleanTweetsRDD)
//    val tweetsWithSentimentDF = tweetsWithSentiment.toDF()
//
//    println(tweetsWithSentiment.count)
//    tweetsWithSentiment.map(_.sentiment).groupBy(identity).mapValues(_.size).foreach(f => println(f))
//    if(safeTweets) IOUtils.DataFrameToJsonFile(tweetsWithSentimentDF, "tweets_clean", true)


    //PartyPrediction
    println("Party prediction")

    val partyPrediction = new PartyPrediction(sparkSession, cleanTweetsRDD.toDF.cache)
//    partyPrediction.topics.foreach(f => println(f))
  }
}
