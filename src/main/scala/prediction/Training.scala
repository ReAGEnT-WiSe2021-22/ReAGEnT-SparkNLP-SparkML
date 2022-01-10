package prediction

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


/**
 * Use the Spark-ML pipeline for training
 * See for more information: https://spark.apache.org/docs/latest/ml-pipeline.html
 * @param raw_data Raw data for Training with several possible parties
 * @param ss SparkSession-object
 */
class Training(raw_data:RDD[TrainingTweet], ss:SparkSession) {

  //Possible parties: "CDU", "SPD", "FDP", "AfD", "Die_Gruenen", "Die_Linke"
  val data_CDU: RDD[TrainingTweet] = prepareData(raw_data, "CDU").cache()
  val data_SPD: RDD[TrainingTweet] = prepareData(raw_data, "SPD").cache()
  val data_FDP: RDD[TrainingTweet] = prepareData(raw_data, "FDP").cache()
  val data_AfD: RDD[TrainingTweet] = prepareData(raw_data, "AfD").cache()
  val data_Die_Gruenen: RDD[TrainingTweet] = prepareData(raw_data, "Die_Gruenen").cache()
  val data_Die_Linke: RDD[TrainingTweet] = prepareData(raw_data, "Die_Linke").cache()


  /**
   * First filter by a specific party
   * You can filter for:
   * "CDU", "SPD", "AfD", "FDP", "Die_Grunen", "Die_Linke"
   *
   * Then group by the creation dates
   * After that the average sentiment value of each day will be calculated
   * At the end sort by the creation dates of the tweets (beginning with 1st January)
   *
   * For the new TrainingTweet object, the  text of the "head" tweet will be used, since the text is irrelevant
   *
   * @param rdd RDD with training tweets
   * @param party to filter for
   * @return RDD with average sentiment values per day for a specific party
   */
  def prepareData(rdd:RDD[TrainingTweet], party:String):RDD[TrainingTweet] = {
    if(rdd.isEmpty()) throw new Error("No preparation possible, RDD is empty")

    rdd.filter(x => x.party.equals(party))
      .groupBy(x => x.date) //RDD[(Date, Iterable[TrainingTweet])]
      .map(tweetsWithSameCreationDate => {
        val head = tweetsWithSameCreationDate._2.head
        val sentiments = tweetsWithSameCreationDate._2.map(_.sentiment)
        val average = sentiments.sum/sentiments.size
        TrainingTweet(party, head.text, head.date, average)
      })
      .sortBy(x => x.date.toLocalDate.toEpochDay)
  }

  /**
   * Create Dataframe from RDD, transform the data and pass it to the estimater (LinearRegression)
   * @param rdd TrainingData of one specific party
   * @return Model as dataframe-object with columns:
   *         "features", "dateformats", "label","features_squared", "features_cubic"
   *         "transformed_features", "prediction"
   */
  def trainModel(rdd:RDD[TrainingTweet]):DataFrame = {
    if (rdd.isEmpty()) throw new Error("Training not possible, RDD is empty")

    val relevantData = rdd.map(x => ( Training.downsize(x.date.toLocalDate.toEpochDay), x.date, x.sentiment))
    //(Double, Date, Double)

    //Create Dataframe from RDD
    val df = ss.createDataFrame(relevantData)
      .withColumnRenamed("_1", "features")
      .withColumnRenamed("_2", "dateformats")
      .withColumnRenamed("_3", "label") //sentiments
      .withColumn("features_squared", functions.pow( col("features"), lit(2) ))
      .withColumn("features_cubic", functions.pow( col("features"), lit(3) ))
      .cache()

    //Transformer
    //Model: y = a*x^3 + b*x^2 + c*x + d
    val transformedData = new VectorAssembler()
      .setInputCols(Array("features", "features_squared", "features_cubic"))
      .setOutputCol("transformed_features")
      .transform(df)
      .cache()

    //Estimater
    val linreg = new LinearRegression()
      .setFeaturesCol("transformed_features")
      .setLabelCol("label")
      .setMaxIter(10)

    //Validation - more info about validation here: https://spark.apache.org/docs/latest/ml-tuning.html
    val paramGrid = new ParamGridBuilder()
      .addGrid(linreg.regParam, Array(0.1, 0.01))
      .addGrid(linreg.fitIntercept)
      .addGrid(linreg.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(linreg)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    val model = trainValidationSplit.fit(transformedData)
    //Calculate predictions
    val result_model = model.transform(transformedData)

    //Evaluation
    val eval = new RegressionEvaluator()
    println("Evaluation: " + eval.evaluate(result_model))

    //println("Large better? " + eval.isLargerBetter)
    //Just for testing

    result_model
  }
}

/**
 * Object with helper functions
 */
object Training {

  def getSentiments(rdd:RDD[TrainingTweet]):Array[Double] = {
    if (rdd.isEmpty()) throw new Error("No sentiment values found, RDD is empty")
      rdd.map(tweet => tweet.sentiment).collect()
    }

  def getDates(rdd:RDD[TrainingTweet]):Array[Double] = {
    if (rdd.isEmpty()) throw new Error("No Dates found, RDD is empty")
    rdd.map( tweet => downsize(tweet.date.toLocalDate.toEpochDay) ).collect()
  }

  /**
   * Long values of the dates are all over 10000, so they will be downsized for convenience
   * @param value Date as long value
   * @return Date as downsized double value
   */
  def downsize(value:Long):Double = value / 1000.0

  /**
   * @param amount How many tweets should be printed
   */
  def printData(rdd:RDD[TrainingTweet], amount:Int = 10):Unit = {
    if (rdd.isEmpty())throw new Error("Printing not possible, RDD is empty")
    rdd.take(amount).foreach(println)
  }

  /**
   * Analyse wether the party reputations had increased or decreased
   * @param df Dataframe with trained model
   * @return True, if sentiment values are increasing
   *         False, if sentiment values are decreasing
   */
  def trendAnalyse(df:DataFrame):Boolean = {
    val predictions = df.select("prediction")
      .rdd
      .map(x => x(0).asInstanceOf[Double])
      .collect()

    predictions.head < predictions.last
  }
}
