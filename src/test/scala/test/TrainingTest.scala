package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import training.{Training, TrainingTweet}
import utils.{IOUtils, TrainingVisualizer, TwitterUtilities}

/**
 * Class to test the loading, preparation, training and visualization of data
 * The tweets will be loaded from a json local file, not from a remote database
 *
 * @author Schander 572893
 */
class TrainingTest extends AnyFunSuite with BeforeAndAfterAll {

  //Variables that will be used later during this test
  var spark:SparkSession = _
  var trainingData:RDD[TrainingTweet] = _
  var data:RDD[TrainingTweet] = _
  var model:DataFrame = _

  val party:String = "FDP"
  val filepath:String = "political_tweets_FDP_test.json"


  /**
   * Set up spark for application
   */
  override protected def beforeAll() {
    val conf:SparkConf = new SparkConf()
    conf.set("spark.executor.memory","6g")
    conf.set("spark.driver.memory", "4g")

    spark = SparkSession.builder.appName("ML_party_reputation_test")
      .master("local[*]").config(conf).getOrCreate
    val sc:SparkContext = spark.sparkContext
  }

  /**
   * Load tweets into RDD from local files
   */
  test("Loading training data"){
    val twitterData:RDD[String] = IOUtils.RDDFromFile(filepath).cache()
    println("--- File read ---")
    trainingData = twitterData.flatMap( TwitterUtilities.parse ).cache()
    println("--- Parsed ---")
  }

  /**
   * Prepare Data for Training and then start training of model
   */
  test("Training of model - Choose model"){
    data = Training.prepareData(trainingData, party).cache()
    model = Training.trainModel(data)
    //Extra evaluation
    println("Increased reputation: " + Training.trendAnalyse(model))
  }

  /**
   * Create two frames with real sentiment values and trained model to visualize training
   */
  test("Visualization"){
    val dates = Training.getDates(data)
    val sentiments = Training.getSentiments(data)
    val predictions = model.select("prediction").collect().map(_.getDouble(0))

    val raw_data_frame = TrainingVisualizer.plotData(dates, sentiments, "Raw Data")
    val prediction_frame = TrainingVisualizer.plotData(dates, predictions, "Prediction")

    println("Please press enter to close frames...")
    System.in.read()

    TrainingVisualizer.disposeFrame(raw_data_frame)
    TrainingVisualizer.disposeFrame(prediction_frame)
  }


  /**
   * Stop Spark after the application has finished
   */
  override protected def afterAll() {
    if (spark != null) {
      spark.stop()
      // If 'Goodbye' was printed, the programm had finished successfully
      println("Goodbye Test")
    }
    else println("Cannot stop spark - No reference")
  }
}
