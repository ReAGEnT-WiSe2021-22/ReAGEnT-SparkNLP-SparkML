package training

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.SparkContext
import utils.TweetLoader
import org.apache.spark.rdd.RDD
import org.bson.Document

import java.io.{FileWriter, PrintWriter}
import scala.collection.JavaConverters._


/**
 * Class which uses machine learning to analyze the reputation of German political parties in 2021
 * At first the tweets will be loaded from the database collection "political_tweets_2021" which contains all required tweets
 * Then the tweets will be prepared and then handed over to the training
 * For each party, there will be an own training
 * During the training, for each date a prediction will be calculated, every model will be saved in dataframe-object
 * At the end the dataframes with models will be written to the database
 *
 * Dates & predictions will be written to the collection: ml_party_reputation_predictions
 * Dates & original sentiment values (labels) will be written to the collections: ml_party_reputation_labels
 *
 * @author Schander 572893
 */
object Main {

  /**
   * Load tweets from DB, prepare Tweets, start Training, write model back to DB
   * @param args Not used
   */
  def main(args: Array[String]):Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local")
      .appName("ML_party_reputation")
      .config("spark.mongodb.input.uri", sys.env("REAGENT_MONGO") + "examples.political_tweets_2021?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    // Load tweets from DB
    val tweets = MongoSpark.load(spark)
    tweets.createOrReplaceTempView("tweets")

    // Create SparkContext
    val sc = spark.sparkContext
    val rdd:RDD[Document] = MongoSpark.load(sc).rdd.cache()

    println("##### TweetLoader  #####")
    val trainingData:RDD[TrainingTweet] = TweetLoader.prepareTweets(rdd).cache()


    println("##### Preparation of data #####")
    //Possible parties: "CDU", "SPD", "FDP", "AfD", "Die_Gruenen", "Die_Linke"
    val data_CDU = Training.prepareData(trainingData, "CDU").cache()
    val data_SPD = Training.prepareData(trainingData, "SPD").cache()
    val data_FDP = Training.prepareData(trainingData, "FDP").cache()
    val data_AfD = Training.prepareData(trainingData, "AfD").cache()
    val data_Die_Gruenen= Training.prepareData(trainingData, "Die_Gruenen").cache()
    val data_Die_Linke = Training.prepareData(trainingData, "Die_Linke").cache()

    println("##### Training #####")
    val trained_model_CDU = Training.trainModel(data_CDU).cache()
    val trained_model_SPD = Training.trainModel(data_SPD).cache()
    val trained_model_FDP = Training.trainModel(data_FDP).cache()
    val trained_model_AfD = Training.trainModel(data_AfD).cache()
    val trained_model_Die_Gruenen = Training.trainModel(data_Die_Gruenen).cache()
    val trained_model_Die_Linke = Training.trainModel(data_Die_Linke).cache()

    // Extra evaluation
    println("Increased reputation CDU: " + Training.trendAnalyse(trained_model_CDU))
    println("Increased reputation SPD: " + Training.trendAnalyse(trained_model_SPD))
    println("Increased reputation FDP: " + Training.trendAnalyse(trained_model_FDP))
    println("Increased reputation AfD: " + Training.trendAnalyse(trained_model_AfD))
    println("Increased reputation Die_Gruenen: " + Training.trendAnalyse(trained_model_Die_Gruenen))
    println("Increased reputation Die_Linke: " + Training.trendAnalyse(trained_model_Die_Linke))


    val mongoData_CDU_pred = createRDDWithDocument(trained_model_CDU, "CDU", selectPredictions = true).cache()
    val mongoData_CDU_lab = createRDDWithDocument(trained_model_CDU, "CDU", selectPredictions = false).cache()

    val mongoData_SPD_pred = createRDDWithDocument(trained_model_SPD, "SPD", selectPredictions = true).cache()
    val mongoData_SPD_lab = createRDDWithDocument(trained_model_SPD, "SPD", selectPredictions = false).cache()

    val mongoData_FDP_pred = createRDDWithDocument(trained_model_FDP, "FDP", selectPredictions = true).cache()
    val mongoData_FDP_lab = createRDDWithDocument(trained_model_FDP, "FDP", selectPredictions = false).cache()

    val mongoData_AfD_pred = createRDDWithDocument(trained_model_AfD, "AfD", selectPredictions = true).cache()
    val mongoData_AfD_lab = createRDDWithDocument(trained_model_AfD, "AfD", selectPredictions = false).cache()

    val mongoData_Die_Gruenen_pred = createRDDWithDocument(trained_model_Die_Gruenen, "Die_Gruenen", selectPredictions = true).cache()
    val mongoData_Die_Gruenen_lab = createRDDWithDocument(trained_model_Die_Gruenen, "Die_Gruenen", selectPredictions = false).cache()

    val mongoData_Die_Linke_pred = createRDDWithDocument(trained_model_Die_Linke, "Die_Linke", selectPredictions = true).cache()
    val mongoData_Die_Linke_lab = createRDDWithDocument(trained_model_Die_Linke, "Die_Linke", selectPredictions = false).cache()


    // Load models into MongoDB collections: "ml_party_reputation_predictions & ml_party_reputation_labels"
    val mongoData_predictions = mongoData_CDU_pred
      .union(mongoData_SPD_pred)
      .union(mongoData_FDP_pred)
      .union(mongoData_AfD_pred)
      .union(mongoData_Die_Gruenen_pred)
      .union(mongoData_Die_Linke_pred)
      .cache()

    val mongoData_labels = mongoData_CDU_lab
      .union(mongoData_SPD_lab)
      .union(mongoData_FDP_lab)
      .union(mongoData_AfD_lab)
      .union(mongoData_Die_Gruenen_lab)
      .union(mongoData_Die_Linke_lab)
      .cache()

    println("##### Writing to MongoDB #####")
    mongoData_predictions.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_predictions?authSource=examples"))))
    mongoData_labels.saveToMongoDB(WriteConfig(Map("uri" -> (sys.env("REAGENT_MONGO") + "examples.ml_party_reputation_labels?authSource=examples"))))

    /*
        // If you want to save the model in a local file, you can uncomment this section
        println("##### Writing to local files #####")
        writeModelToFile(trained_model_CDU, "CDU", selectPredictions = true)
        writeModelToFile(trained_model_CDU, "CDU", selectPredictions = false)

        writeModelToFile(trained_model_SPD, "SPD", selectPredictions = true)
        writeModelToFile(trained_model_SPD, "SPD", selectPredictions = false)

        writeModelToFile(trained_model_FDP, "FDP", selectPredictions = true)
        writeModelToFile(trained_model_FDP, "FDP", selectPredictions = false)

        writeModelToFile(trained_model_AfD, "AfD", selectPredictions = true)
        writeModelToFile(trained_model_AfD, "AfD", selectPredictions = false)

        writeModelToFile(trained_model_Die_Gruenen, "Die_Gruenen", selectPredictions = true)
        writeModelToFile(trained_model_Die_Gruenen, "Die_Gruenen", selectPredictions = false)

        writeModelToFile(trained_model_Die_Linke, "Die_Linke", selectPredictions = true)
        writeModelToFile(trained_model_Die_Linke, "Die_Linke", selectPredictions = false)
    */


    spark.stop()
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
    if(selectPredictions) values = model.select("prediction").collect().map(_.getDouble(0)).toList
    else values = model.select("label").collect().map(_.getDouble(0)).toList

    val document = new Document("party", party).append("dates", dates.asJava).append("values", values.asJava)
    val seq = Seq(document)
    SparkContext.getOrCreate().parallelize(seq)
  }

  /**
   * Writes local model into local json files, just used for testing
   * @param model Dataframe with trained model
   * @param party Party of the model
   * @param selectPredictions If true, predictions will be selected,
   *                          if false, the original sentiment values ("label") will be selected
   */
  def writeModelToFile(model:DataFrame, party:String, selectPredictions:Boolean):Unit = {
    val dates = model.select("dateformats").collect().map(_(0).toString).toList
    var values:List[Double] = List()
    var file:String = ""

    if(selectPredictions) {
      values = model.select("prediction").collect().map(_.getDouble(0)).toList
      file = "trained_model_predictions.json"
    }
    else {
      values = model.select("label").collect().map(_.getDouble(0)).toList
      file = "trained_model_labels.json"
    }

    val documentString = new Document("party", party).append("dates", dates.asJava).append("values", values.asJava).toJson
    val fw = new FileWriter(file, true)
    val pw = new PrintWriter(fw)
    pw.write(documentString + System.lineSeparator())
    pw.close()
  }
}

