package prediction

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import utils.{IOUtils, JSONUtils, SentimentAnalysisUtils, Sentiments, TwitterUtilities}
import org.apache.spark.rdd.RDD
import edu.stanford.nlp.pipeline.StanfordCoreNLP


/**
 * At first political tweets from 2021 will be loaded from the database
 * Then the tweets will be prepared and handed over to the training
 */
object Main {

  def main(args: Array[String]):Unit = {

    /*
    Will be used later

    // Create Sparksession
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Prediction_of_party_reputation")
      .config("spark.mongodb.input.uri", sys.env("REAGENT_MONGO") + "examples.political_tweets_2021?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    // Load tweets from DB
    val tweets = MongoSpark.load(sparkSession)
    tweets.createOrReplaceTempView("tweets")

    // Create SparkSession
    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd.cache() //RDD[Document]

    val trainingData:RDD[TrainingTweet] = TweetLoader.prepareTweets(rdd).cache()

     */

    //For now, just use local tweets

    val conf:SparkConf = new SparkConf()
    conf.set("spark.executor.memory","6g")
    conf.set("spark.driver.memory", "4g")

    val ss:SparkSession= SparkSession.builder.appName("SparkLocal").
      master("local[*]").config(conf).getOrCreate
    val sc: SparkContext = ss.sparkContext


    // Message: Exception in thread "sbt-bg-threads-1" java.lang.OutOfMemoryError: GC overhead limit exceeded
    // val sentimentUtil = new SentimentAnalysisUtils() GC overhead limit exceeded

    val twitterData:RDD[String] = IOUtils.RDDFromFile("political_tweets_test.json",false).cache()
    println("File read")
    val trainingData:RDD[TrainingTweet] = twitterData.flatMap(TwitterUtilities.parse).cache()
    println("Parsed")

    val tweet = trainingData.first()
    println("First tweet: \n", tweet)

    val train = new Training(trainingData)
    train.printData(train.Data_FDP)
    train.plotData(train.Data_FDP)


    //TODO: Training


    // If 'Goodbye' was printed, the programm had finished successfully
    ss.stop()
    println("Goodbye")
  }
}

//TODO: TweetLoaderTest
