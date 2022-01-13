package prediction

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.{IOUtils, TrainingVisualizer, TweetLoader, TwitterUtilities}
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConverters._


/**
 * At first political tweets from 2021 will be loaded from the database
 * Then the tweets will be prepared and handed over to the training
 * During the training, for each date a prediction will be calculated, everything will be saved in a dataframe
 * At the end the dataframes with models (for each party) will be written to the database
 *
 * Dates & predictions will be written to the collection: ml_party_reputation_predictions
 * Dates & original sentiment values (labels) will be written to the collections: ml_party_reputation_labels
 *
 * For some testing a visualization was added which isn´t needed after deployment
 */
object Main {

  /**
   * Load tweets from DB, prepare Tweets, start Training, write model back to DB
   * @param args Not used
   */
  def main(args: Array[String]):Unit = {

    // Create SparkSession
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("ML_party_reputation")
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

/*
    // --- Just use local tweets for testing --- //

    val conf:SparkConf = new SparkConf()
    conf.set("spark.executor.memory","6g")
    conf.set("spark.driver.memory", "4g")

    val sparkSession:SparkSession= SparkSession.builder.appName("ML_party_reputation")
      .master("local[*]").config(conf).getOrCreate
    val sc:SparkContext = sparkSession.sparkContext


    //About 4900 tweets are saved in political_tweets_test.json for testing
    val twitterData:RDD[String] = IOUtils.RDDFromFile("political_tweets_CDU_test.json", isAResource = false).cache()
    println("--- File read ---")
    val trainingData:RDD[TrainingTweet] = twitterData.flatMap( TwitterUtilities.parse ).cache()
    println("--- Parsed ---")

    // --- End of local tweets for testing --- //
*/

    val train = new Training(trainingData, sparkSession)

    println("--- Training ---")
    val trained_model_CDU = train.trainModel(train.data_CDU).cache()
    val trained_model_SPD = train.trainModel(train.data_SPD).cache()
    val trained_model_FDP = train.trainModel(train.data_FDP).cache()
    val trained_model_AfD = train.trainModel(train.data_AfD).cache()
    val trained_model_Die_Gruenen = train.trainModel(train.data_Die_Gruenen).cache()
    val trained_model_Die_Linke = train.trainModel(train.data_Die_Linke).cache()

    // Extra evaluation
    println("Increased reputation CDU: " + Training.trendAnalyse(trained_model_CDU))
    println("Increased reputation SPD: " + Training.trendAnalyse(trained_model_SPD))
    println("Increased reputation FDP: " + Training.trendAnalyse(trained_model_FDP))
    println("Increased reputation AfD: " + Training.trendAnalyse(trained_model_AfD))
    println("Increased reputation Die_Gruenen: " + Training.trendAnalyse(trained_model_Die_Gruenen))
    println("Increased reputation Die_Linke: " + Training.trendAnalyse(trained_model_Die_Linke))

/*
    // --- Visualization Start, just for local testing --- //

    val dates = Training.getDates(train.data_Die_Gruenen)
    val sentiments = Training.getSentiments(train.data_Die_Gruenen)
    val predictions = trained_model_Die_Gruenen.collect().map(x => x.getDouble(4))

    val raw_data_frame = TrainingVisualizer.plotData(dates, sentiments, "Raw Data")
    val prediction_frame = TrainingVisualizer.plotData(dates, predictions, "Prediction")

    println("Please press enter to close frames...")
    System.in.read()

    TrainingVisualizer.disposeFrame(raw_data_frame)
    TrainingVisualizer.disposeFrame(prediction_frame)

    // --- Visualization End --- //
*/

    // Load models into MongoDB, collection: "ml_party_reputation_predictions & ml_party_reputation_labels"
    val mongoData_CDU_pred = createRDDWithDocument(trained_model_CDU, "CDU", selectPredictions = true).cache()
    mongoData_CDU_pred.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    val mongoData_CDU_lab = createRDDWithDocument(trained_model_CDU, "CDU", selectPredictions = false).cache()
    mongoData_CDU_lab.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))
    println("Data for 'CDU' saved to DB")

    val mongoData_SPD_pred = createRDDWithDocument(trained_model_SPD, "SPD", selectPredictions = true).cache()
    mongoData_SPD_pred.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    val mongoData_SPD_lab = createRDDWithDocument(trained_model_SPD, "SPD", selectPredictions = false).cache()
    mongoData_SPD_lab.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))
    println("Data for 'SPD' saved to DB")

    val mongoData_FDP_pred = createRDDWithDocument(trained_model_FDP, "FDP", selectPredictions = true).cache()
    mongoData_FDP_pred.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    val mongoData_FDP_lab = createRDDWithDocument(trained_model_FDP, "FDP", selectPredictions = false).cache()
    mongoData_FDP_lab.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))
    println("Data for 'FDP' saved to DB")

    val mongoData_AfD_pred = createRDDWithDocument(trained_model_AfD, "AfD", selectPredictions = true).cache()
    mongoData_AfD_pred.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    val mongoData_AfD_lab = createRDDWithDocument(trained_model_AfD, "AfD", selectPredictions = false).cache()
    mongoData_AfD_lab.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))
    println("Data for 'AfD' saved to DB")

    val mongoData_Die_Gruenen_pred = createRDDWithDocument(trained_model_Die_Gruenen, "Die_Gruenen", selectPredictions = true).cache()
    mongoData_Die_Gruenen_pred.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    val mongoData_Die_Gruenen_lab = createRDDWithDocument(trained_model_Die_Gruenen, "Die_Gruenen", selectPredictions = false).cache()
    mongoData_Die_Gruenen_lab.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))
    println("Data for 'Die_Gruenen' saved to DB")

    val mongoData_Die_Linke_pred = createRDDWithDocument(trained_model_Die_Linke, "Die_Linke", selectPredictions = true).cache()
    mongoData_Die_Linke_pred.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    val mongoData_Die_Linke_lab = createRDDWithDocument(trained_model_Die_Linke, "Die_Linke", selectPredictions = false).cache()
    mongoData_Die_Linke_lab.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))
    println("Data for 'Die_Linke' saved to DB")

    sparkSession.stop()
    // If 'Goodbye' was printed, the programm had finished successfully
    println("Goodbye")
  }

  /**
   * Transforms the dataframe with the model to a RDD with party, dates & sentiments
   *
   * @param model Dataframe with trained model
   * @param party Party of the model
   * @param selectPredictions If true, predictions will be selected,
   *                          if false, the original sentiment values ("label") will be selected
   * @return RDD with Document-objects, so saveToMongoDB() can be called
   */
  def createRDDWithDocument(model:DataFrame, party:String, selectPredictions:Boolean):RDD[Document] = {
    val dates = model.select("dateformats").collect().map(_(0).toString).toList
    var values:List[Double] = List()
    if(selectPredictions) values = model.select("prediction").collect().map(_(0).asInstanceOf[Double]).toList
    else values = model.select("label").collect().map(_(0).asInstanceOf[Double]).toList

    val document = new Document("party", party).append("dates", dates.asJava).append("values", values.asJava)
    val seq = Seq(document)
    SparkContext.getOrCreate().parallelize(seq)
  }
}

