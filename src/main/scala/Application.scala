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

      if useLocalTweets is false: sparkSession needs connection to database on production
     */
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

    println("Tweets loaded.")

    //Preprocessing
    val result = new Preprocessing(tweetDF).getResult()
    //transform preprocessing result into Tweets
    val cleanTweetsRDD = result.as[Tweet].rdd

    println("Preprocessed.")
    cleanTweetsRDD.take(5).foreach(f => println(f))

    //Sentiment Analysis
    val tweetsWithSentiment = TwitterUtils.applySentimentToTweets(cleanTweetsRDD)
    val tweetsWithSentimentDF = tweetsWithSentiment.toDF()

    println(tweetsWithSentiment.count)
    println(tweetsWithSentiment.map(x => (x.cleanText, x.sentiment)).filter(_._2 != 2.0).count)
    if(true) IOUtils.DataFrameToJsonFile(tweetsWithSentimentDF, "tweets_clean", true)

    println("Added Sentiment.")

    //PartyPrediction
    //val partyPrediction = new PartyPrediction(sparkSession, cleanTweetsDF)

    //partyPrediction.count_vecDF.select("cleanText", "countVector").take(2).foreach(f => println(f))
    //partyPrediction.topics.foreach(f => println(f))
    //partyPrediction.result.take(5).foreach(f => println(f)
    //partyPrediction.result.select("cleanText", "topicDistribution").take(10).foreach(f => println(f))

    //cleanTweetsRDD.map(x => (x.text, x.cleanText)).filter(x => x._2.contains("rt")).take(5).foreach(println(_))
  }
}
