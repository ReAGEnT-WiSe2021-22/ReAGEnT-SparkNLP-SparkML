package training

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * Class that uses the Spark-ML pipeline for training
 * See for more information: https://spark.apache.org/docs/latest/ml-pipeline.html
 *
 * @author Schander 572893
 */
object Training {

  /**
   * First filter by a specific party. You can filter for:
   * "CDU", "SPD", "AfD", "FDP", "Die_Grunen", "Die_Linke"
   *
   * Then group by the creation dates
   * After that the average sentiment value of each day will be calculated
   * At the end, sort by the creation dates of the tweets (beginning with 1st January)
   *
   * @param rdd RDD with training tweets
   * @param party Party to filter for
   * @return RDD with average sentiment values per day for a specific party
   */
  def prepareData(rdd: RDD[TrainingTweet], party: String): RDD[TrainingTweet] = {
    if (rdd.isEmpty()) throw new Error("No preparation possible, RDD is empty")

    rdd.filter(x => x.party.equals(party))
      .groupBy(x => x.date) //RDD[(Date, Iterable[TrainingTweet])]
      .map(tweetsWithSameCreationDate => {
        val date = tweetsWithSameCreationDate._1
        val sentiments = tweetsWithSameCreationDate._2.map(_.sentiment)
        val average = sentiments.sum / sentiments.size

        TrainingTweet(party, date, average)
      })
      .sortBy(x => x.date.toLocalDate.toEpochDay)
  }

  /**
   * Create Dataframe from RDD, transform the data and pass it to the estimater (LinearRegression)
   *
   * @param rdd TrainingData of one specific party
   * @return Model as dataframe-object with columns:
   *         "features", "dateformats", "label", "transformed_features", "prediction"
   */
  def trainModel(rdd: RDD[TrainingTweet], showEvaluation:Boolean=true): DataFrame = {
    if (rdd.isEmpty()) throw new Error("Training not possible, RDD is empty")

    val relevantData = rdd.map(x => (Training.downsize(x.date.toLocalDate.toEpochDay), x.date, x.sentiment))
    //(Double, Date, Double)

    //Create Dataframe from RDD
    val spark = SparkSession.getActiveSession.get
    val df = spark.createDataFrame(relevantData)
      .withColumnRenamed("_1", "features")
      .withColumnRenamed("_2", "dateformats")
      .withColumnRenamed("_3", "label") //original sentiments
      .cache()

    //Transformer
    //Old Model: y = a*x^3 + b*x^2 + c*x + d This model was discarded since it´s just overfitting in this case
    //New Model: y = a*x + b
    val transformedData = new VectorAssembler()
      .setInputCols(Array("features"))
      .setOutputCol("transformed_features")
      .transform(df)
      .cache()

    //Estimater - Default iterations: 100
    val linreg = new LinearRegression()
      .setFeaturesCol("transformed_features")
      .setLabelCol("label")

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
    if(showEvaluation) {
      val eval = new RegressionEvaluator()
      println(">>> Evaluation: " + eval.evaluate(result_model))
      println("Large better? " + eval.isLargerBetter)
    }

    result_model
  }


  // --- Helper functions --- //

  /**
   * @param rdd RDD with TrainingTweets
   * @return Array of all sentiment values of each tweet
   */
  def getSentiments(rdd: RDD[TrainingTweet]): Array[Double] = {
    if (rdd.isEmpty()) throw new Error("No sentiment values found, RDD is empty")
    rdd.map(tweet => tweet.sentiment).collect()
  }

  /**
   * @param rdd RDD with TrainingTweets
   * @return Array of all Dates of each tweet
   */
  def getDates(rdd: RDD[TrainingTweet]): Array[Double] = {
    if (rdd.isEmpty()) throw new Error("No Dates found, RDD is empty")
    rdd.map(tweet => downsize(tweet.date.toLocalDate.toEpochDay)).collect()
  }

  /**
   * Long values of the dates are all over 10000, so they will be downsized for convenience
   *
   * @param value Date as long value
   * @return Date as downsized double value
   */
  def downsize(value: Long): Double = value / 1000.0

  /**
   * @param amount How many tweets should be printed
   */
  def printData(rdd: RDD[TrainingTweet], amount: Int = 10): Unit = {
    if (rdd.isEmpty()) throw new Error("Printing not possible, RDD is empty")
    rdd.take(amount).foreach(println)
  }

  /**
   * Analyse whether the party reputations had increased or decreased
   *
   * @param df Dataframe with trained model
   * @return True, if sentiment values are increasing
   *         False, if sentiment values are decreasing or constant
   */
  def trendAnalyse(df: DataFrame): Boolean = {
    val predictions = df.select("prediction")
      .rdd
      .map(_.getDouble(0))
      .collect()

    predictions.head < predictions.last
  }
}