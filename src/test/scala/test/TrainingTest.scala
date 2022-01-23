package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import training.{Training, TrainingTweet}
import utils.{IOUtils, TrainingVisualizer, TwitterUtilities}

/**
 * Class to test the loading, preparation, training and visualization of the data
 * The tweets will be loaded from local files, not from a remote database
 * You can choose which party should be used for this test in the variables
 * "filepath" & "party"
 */
class TrainingTest extends AnyFunSuite with BeforeAndAfterAll {

  //Variables that will be used later during this test
  var spark:SparkSession = _
  var trainingData:RDD[TrainingTweet] = _
  var data:RDD[TrainingTweet] = _
  var model:DataFrame = _

  /**
   * Choose which file and which party should be used for the test
   * Normally you just have to change the party name in the filepath, for example "_CDU_" or "_SPD_"
   */
  val filepath = "example_tweets/political_tweets_CDU_2021_cleaned.json"
  val party = "CDU"


  /**
   * Set up spark for application
   */
  override protected def beforeAll() {
    val conf:SparkConf = new SparkConf()
    conf.set("spark.executor.memory","6g")
    conf.set("spark.driver.memory", "4g")

    spark = SparkSession.builder.appName("ML_party_reputation")
      .master("local[*]").config(conf).getOrCreate
    val sc:SparkContext = spark.sparkContext
  }

  /**
   * Load tweets into RDD from local files
   */
  test("Loading training data"){
    val twitterData:RDD[String] = IOUtils.RDDFromFile(filepath, isAResource = false).cache()
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
    //val predictions = model.collect().map(x => x.getDouble(4))
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
    if (spark!=null) {
      spark.stop()
      // If 'Goodbye' was printed, the programm had finished successfully
      println("Goodbye")
    }
    else println("Cannot stop spark - No reference")
  }
}
