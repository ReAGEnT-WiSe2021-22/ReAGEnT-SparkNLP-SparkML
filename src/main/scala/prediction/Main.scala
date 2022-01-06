package prediction

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import utils.{IOUtils, TwitterUtilities}
import org.apache.spark.rdd.RDD


/**
 * At first political tweets from 2021 will be loaded from the database
 * Then the tweets will be prepared and handed over to the training
 * At the end the models will be written to the database
 */
object Main {

  def main(args: Array[String]):Unit = {


    /*Will be used later

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

    // Create SparkContext
    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd.cache() //RDD[Document]

    val trainingData:RDD[TrainingTweet] = TweetLoader.prepareTweets(rdd).cache()
    println("##### TweetLoader finished #####")

    */

    //For now, just use local tweets

    val conf:SparkConf = new SparkConf()
    conf.set("spark.executor.memory","6g")
    conf.set("spark.driver.memory", "4g")

    val ss:SparkSession= SparkSession.builder.appName("Predicition_of_party_reputation")
      .master("local[*]").config(conf).getOrCreate
    val sc:SparkContext = ss.sparkContext


    val twitterData:RDD[String] = IOUtils.RDDFromFile("political_tweets_test.json",false).cache()
    println("File read")
    val trainingData:RDD[TrainingTweet] = twitterData.flatMap( TwitterUtilities.parse ).cache()
    println("Parsed")


    val train = new Training(trainingData, ss)

    val data_rdd = train.data_CDU
    val dates = Training.getDates(data_rdd)
    val sentiments = Training.getSentiments(data_rdd)


    println("--- Training ---")
    val result_model = train.trainModel(data_rdd).cache()
    println("Increased reputation: " + Training.trendAnalyse(result_model))


    // TODO Write to MongoDB


    // --- Visualization --- //

    val prediction = result_model.collect().map(x => x.get(6).asInstanceOf[Double])

    val rawData_frame = Training.plotData(dates, sentiments, "Raw Data")
    val prediction_frame = Training.plotData(dates, prediction, "Prediction")


    println("Please press enter to close frames...")
    System.in.read()

    rawData_frame.setVisible(false)
    rawData_frame.dispose()
    prediction_frame.setVisible(false)
    prediction_frame.dispose()


    ss.stop()
    // If 'Goodbye' was printed, the programm had finished successfully
    println("Goodbye")
  }
}

// TODO TweetLoaderTest
