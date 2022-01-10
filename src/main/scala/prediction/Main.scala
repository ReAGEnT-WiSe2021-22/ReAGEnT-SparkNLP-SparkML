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
 * During the training, for each date a prediction will be calculated, everything will be saved in dataframe
 * At the end the dataframes with models (for each party) will be written to the database
 *
 * For some testing a visualization was added which isn´t needed after deployment
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

    //Just use local tweets

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

    println("--- Training ---")
    val trained_model_CDU = train.trainModel(train.data_CDU).cache()
    val trained_model_SPD = train.trainModel(train.data_SPD).cache()
    val trained_model_FDP = train.trainModel(train.data_FDP).cache()
    val trained_model_AfD = train.trainModel(train.data_AfD).cache()
    val trained_model_Die_Gruenen = train.trainModel(train.data_Die_Gruenen).cache()
    val trained_model_Die_Linke = train.trainModel(train.data_Die_Linke).cache()


    // --- Visualization Start, just for Testing --- //

    println("Increased reputation: " + Training.trendAnalyse(trained_model_CDU))

    val dates = Training.getDates(train.data_CDU)
    val sentiments = Training.getSentiments(train.data_CDU)
    val predictions = trained_model_CDU.collect().map(x => x.get(6).asInstanceOf[Double])

    val raw_data_frame = TrainingVisualizer.plotData(dates, sentiments, "Raw Data")
    val prediction_frame = TrainingVisualizer.plotData(dates, predictions, "Prediction")

    println("Please press enter to close frames...")
    System.in.read()

    TrainingVisualizer.disposeFrame(raw_data_frame)
    TrainingVisualizer.disposeFrame(prediction_frame)

    // --- Visualization End --- //

    //Load models into MongoDB, collection: "ml_party_reputation"
    val mongoData_CDU = createRDDWithDocuments(trained_model_CDU, "CDU", sparkSession)
    mongoData_CDU.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation?authSource=examples"))))
    val mongoData_SPD = createRDDWithDocuments(trained_model_SPD, "SPD", sparkSession)
    mongoData_SPD.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation?authSource=examples"))))
    val mongoData_FDP = createRDDWithDocuments(trained_model_FDP, "FDP", sparkSession)
    mongoData_FDP.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation?authSource=examples"))))
    val mongoData_AfD = createRDDWithDocuments(trained_model_AfD, "AfD", sparkSession)
    mongoData_AfD.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation?authSource=examples"))))
    val mongoData_Die_Gruenen = createRDDWithDocuments(trained_model_Die_Gruenen, "Die_Gruenen", sparkSession)
    mongoData_Die_Gruenen.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation?authSource=examples"))))
    val mongoData_Die_Linke = createRDDWithDocuments(trained_model_Die_Linke, "Die_Linke", sparkSession)
    mongoData_Die_Linke.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation?authSource=examples"))))


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
