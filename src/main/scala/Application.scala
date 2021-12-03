import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import domain.Tweet
import nlp.{Preprocessing, SentimentAnalysis}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils._

object Application {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("PartyPrediction")
//      .config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.tweets")
//      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    /*
      Loading the data

      if useLocalTweets is false: sparkSession needs connection to database on production
     */
    var tweetDF:DataFrame = null
    val useLocalTweets = true
    val safeTweets = false

    if (useLocalTweets) {
      val tweetRDD = IOUtils.RDDFromJsonFile[Tweet]("tweets_raw", true)
      tweetDF = tweetRDD.toDF().cache
    }
    else {
      val rdd = MongoSpark.load(sc).rdd
      tweetDF = TwitterUtils.parseDocumentToTweet(rdd).toDF().cache

      val tweetsPath = "src/main/resources/tweets_raw.json"
      if (safeTweets) IOUtils.DataFrameToJsonFile(tweetDF, tweetsPath, true)
    }

    //Preprocessing
    val result = new Preprocessing(tweetDF).getResult()
    //transform preprocessing result into Tweets
    val cleanTweets:RDD[Tweet] = result.as[Tweet].rdd
    cleanTweets.first()
    if(safeTweets) IOUtils.DataFrameToJsonFile(tweetDF, "tweets_clean", true)

    //Sentiment analysis
//    val tfidfScore = SentimentAnalysis.getT(cleanTweets)
//    println(tfidfScore.first())
  }
}
