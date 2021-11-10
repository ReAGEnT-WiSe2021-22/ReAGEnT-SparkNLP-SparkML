import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import domain.Tweet
import nlp.Preprocessing
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils._

object Application {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("PartyPrediction")
      //.config("spark.mongodb.input.uri", "mongodb://phillip:8hVnKoqd@reagent1.f4.htw-berlin.de:27017/examples.tweets")
      //.config("spark.testing.memory", 2147480000)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    import sparkSession.implicits._


    //load data
    var tweetDF:DataFrame = null
    val useLocalTweets = true

    if (useLocalTweets) {
      val tweetRDD = IOUtils.RDDFromJsonFile[Tweet]("tweets_raw", true)
      tweetDF = tweetRDD.toDF().cache
    }
    else {
      val rdd = MongoSpark.load(sc).rdd
      tweetDF = TwitterUtils.parseDocumentToTweet(rdd).toDF().cache

      val safeTweets = false
      val tweetsPath = "src/main/resources/tweets_raw.json"
      if (safeTweets) IOUtils.DataFrameToJsonFile(tweetDF, tweetsPath, true)
    }

    //Preprocessing
    val result = new Preprocessing(tweetDF).getResult()
    val cleanTweets:RDD[Tweet] = result.as[Tweet].rdd
    //cleanTweets.take(5).foreach(println)

  }
}
