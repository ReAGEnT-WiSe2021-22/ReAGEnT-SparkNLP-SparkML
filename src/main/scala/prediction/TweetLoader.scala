package prediction

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark

object TweetLoader {

  def main(args: Array[String]):Unit = {

    // Sparksession erstellen
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", sys.env("REAGENT_MONGO") + "examples.political_tweets_2021?authSource=examples")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    // Laden der Tweets aus der DB
    val tweets = MongoSpark.load(sparkSession)
    tweets.createOrReplaceTempView("tweets")

    // SparkContext erstellen
    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd


    //TODO:
    // 1. Tweets filtern
    // 2. Sentiments hinzufügen
    // 3. Training


    // Wenn 'Goodbye' geprintet wird, ist das Programm erfolgreich durchgelaufen
    println("Goodbye")
  }
}
