package prediction

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.{IOUtils, TwitterUtilities}
import org.apache.spark.rdd.RDD
import org.bson.Document

import java.sql.Date


/**
 * At first political tweets from 2021 will be loaded from the database
 * Then the tweets will be prepared and handed over to the training
 * At the end the models will be written to the database
 */
object Main {

  /**
   *
   * @param args Unused
   */
  def main(args: Array[String]):Unit = {


    /* Will be used later

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

    val sparkSession:SparkSession= SparkSession.builder.appName("Predicition_of_party_reputation")
      .master("local[*]").config(conf).getOrCreate
    val sc:SparkContext = sparkSession.sparkContext


    val twitterData:RDD[String] = IOUtils.RDDFromFile("political_tweets_test.json",false).cache()
    println("--- File read ---")
    val trainingData:RDD[TrainingTweet] = twitterData.flatMap( TwitterUtilities.parse ).cache()
    println("--- Parsed ---")


    val train = new Training(trainingData, sparkSession)

    val data_rdd = train.data_CDU

    println("--- Training ---")
    val result_model = train.trainModel(data_rdd).cache()
    println("Increased reputation: " + Training.trendAnalyse(result_model))


    // --- Visualization Start --- //

    val dates = Training.getDates(data_rdd)
    val sentiments = Training.getSentiments(data_rdd)
    val predictions = result_model.collect().map(x => x.get(6).asInstanceOf[Double])

    val raw_data_frame = TrainingVisualizer.plotData(dates, sentiments, "Raw Data")
    val prediction_frame = TrainingVisualizer.plotData(dates, predictions, "Prediction")

    println("Please press enter to close frames...")
    System.in.read()

    TrainingVisualizer.disposeFrame(raw_data_frame)
    TrainingVisualizer.disposeFrame(prediction_frame)

    // --- Visualization End --- //

    val mongoData = createRDDWithDocuments(result_model, "CDU", sparkSession)
    //mongoData.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.mostTweetsDayByYear?authSource=examples"))))


    sparkSession.stop()
    // If 'Goodbye' was printed, the programm had finished successfully
    println("Goodbye")
  }

  /**
   * Transforms the dataframe with the model to a RDD with party, dates & sentiments
   *
   * @param model dataframe with trained model
   * @param party party of the model
   * @param sparkSession sparksession-object
   * @return RDD with Document-objects, so saveToMongoDB() can be called
   */
  def createRDDWithDocuments(model:DataFrame, party:String, sparkSession: SparkSession):RDD[Document] = {
    //Einzelne Elemente
    val data = model.collect()
      .map(x => (
        x.getDate(1).toString, //Dates
        x.getDouble(6) //Predictions
      ))
    val rdd = sparkSession.sparkContext.parallelize(data)
    rdd.map(x => Document.parse("{partei: \"" + party + "\", date: \"" + x._1 + "\", sentiment: " + x._2 + "}"))

    /*
    //Listen
    val dates = model.select("dateformates").collect().map(_(0).toString).toList
    val predictions = model.select("prediction").collect().map(_(0).asInstanceOf[Double]).toList
    val seq = Seq(new Document(party, dates.asJava), new Document(party, predictions.asJava))
    sparkSession.sparkContext.parallelize(seq)
     */
  }
}


// TODO TweetLoaderTest
