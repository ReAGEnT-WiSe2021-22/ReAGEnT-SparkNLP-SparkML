package prediction

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.jfree.data.xy.DefaultXYDataset
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.PlotOrientation

import javax.swing.{JFrame, WindowConstants}

/**
 * Use the Spark-ML pipeline for training
 * See for more information: https://spark.apache.org/docs/latest/ml-pipeline.html
 * @param data Raw data for Training saved as RDD
 * @param ss SparkSession-object
 */
class Training(data:RDD[TrainingTweet], ss:SparkSession) {

  /*
    For now every RDD will be split by party
    Could change later...
   */

  val data_CDU: RDD[TrainingTweet] = prepareData(data, "CDU").cache()
  val data_SPD: RDD[TrainingTweet] = prepareData(data, "SPD").cache()
  val data_FDP: RDD[TrainingTweet] = prepareData(data, "FDP").cache()


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
    rdd.filter(x => x.party.equals(party))
      .groupBy(x => x.date)
      .map(tweetsWithSameCreationDate => {
        val head = tweetsWithSameCreationDate._2.head
        val sentiments = tweetsWithSameCreationDate._2.map(_.sentiment)
        //save tweet with average sentiment value per day
        TrainingTweet(party, head.text, head.date, sentiments.sum/sentiments.size)
      })
      .sortBy(x => x.date.toLocalDate.toEpochDay)
  }


  /**
   * @param amount How many tweets should be printed
   */
  def printData(rdd:RDD[TrainingTweet], amount:Int = 10):Unit = rdd.take(amount).foreach(println)

  /**
   * Shows a line graph with sentiment values (y axix) depending on a specific date (x axis)
   * @param dates x values
   * @param sentiments y values
   * @param title title
   * @return JFrame object, that should be disposed later from the outside
   */
  def plotData(dates:Array[Double], sentiments:Array[Double], title:String):JFrame = {
    val dataArray = Array.ofDim[Double](2, sentiments.length)

    dataArray(0) = dates      // x values
    dataArray(1) = sentiments // y values

    val dataset = new DefaultXYDataset
    dataset.addSeries("Training Data", dataArray)
    val plotTitle = title
    val xaxis = "dates"
    val yaxis = "sentiments"
    val orientation = PlotOrientation.VERTICAL
    val show  = false
    val toolTips = false
    val urls = false

    val chart:JFreeChart= ChartFactory.createXYLineChart( plotTitle, xaxis, yaxis,
      dataset, orientation, show, toolTips, urls)

    val frame:JFrame = new JFrame("Data")
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)

    val chartPanel: ChartPanel = new ChartPanel(chart)
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)

    frame
  }

  /**
   * Create Dataframe from RDD, transform the data and pass it to the estimater (LinearRegression)
   * @param rdd TrainingData
   * @return Model as dataframe-object with columns:
   *         "features", "dateformats", "label","features_squared", "features_cubic"
   *         "transformed_features", "prediction"
   */
  def trainModel(rdd:RDD[TrainingTweet]):DataFrame = {
    val relevantData = rdd.map(x => ( Training.downsize(x.date.toLocalDate.toEpochDay), x.date, x.sentiment))

    val func:UserDefinedFunction = udf((value:Double, exponent:Int) => {
      Math.pow(value, exponent)
    })

    //Create Dataframe from RDD
    val df = ss.createDataFrame(relevantData)
      .withColumnRenamed("_1", "features")
      .withColumnRenamed("_2", "dateformats")
      .withColumnRenamed("_3", "label") //sentiments
      .withColumn("features_squared", functions.pow( col("features"), lit(2) ))
      .withColumn("features_cubic", functions.pow( col("features"), lit(3) ))
      .cache()


    //Transformer
    val transformedData = new VectorAssembler()
      .setInputCols(Array("features", "features_squared", "features_cubic"))
      .setOutputCol("transformed_features")
      .transform(df)
      .cache()

    val lr = new LinearRegression()
      .setFeaturesCol("transformed_features")
      .setLabelCol("label")
      .setMaxIter(10)

    //Validation - more info about validation here: https://spark.apache.org/docs/latest/ml-tuning.html
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    val model = trainValidationSplit.fit(transformedData)
    val result_model = model.transform(transformedData)

    //Evaluation
    val eval = new RegressionEvaluator()
    println( "Evaluation: ", eval.evaluate(result_model) )
    println("Large better? ", eval.isLargerBetter)

    result_model
  }
}

object Training {

  def getSentiments(rdd:RDD[TrainingTweet]):Array[Double] = rdd.map(tweet => tweet.sentiment).collect()

  def getDates(rdd:RDD[TrainingTweet]):Array[Double] =
    rdd.map( tweet => downsize(tweet.date.toLocalDate.toEpochDay) ).collect()

  /**
   * Long values of the dates are all over 10000, so they will be downsized for convenience
   * @param value Date as long value
   * @return Date as downsized double value
   */
  def downsize(value:Long):Double = value / 1000.0
}
