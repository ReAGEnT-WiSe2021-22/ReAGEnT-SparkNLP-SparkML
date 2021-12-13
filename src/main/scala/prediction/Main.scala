package prediction

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark

/**
 * At first political tweets from 2021 will be loaded from the database
 * Then the tweets will be prepared and handed over to the training
 */
object Main {

  def main(args: Array[String]):Unit = {

    // Create Sparksession
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", sys.env("REAGENT_MONGO") + "examples.political_tweets_2021?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    // Load tweets from DB
    val tweets = MongoSpark.load(sparkSession)
    tweets.createOrReplaceTempView("tweets")

    // Create SparkSession
    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd //RDD[Document]


    val trainingData = TweetLoader.prepareTweets(rdd)


    //TODO: Training


    // If 'Goodbye' was printed, the programm had finished successfully
    println("Goodbye")
  }
}
