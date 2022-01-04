package prediction

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.jfree.data.xy.DefaultXYDataset
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.PlotOrientation

import javax.swing.{JFrame, WindowConstants}

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
        TrainingTweet(head.party, head.text, head.date, sentiments.sum/sentiments.size)
      })
      .sortBy(x => x.date.toLocalDate.toEpochDay)
  }


  /**
   * @param amount How many tweets should be printed
   */
  def printData(rdd:RDD[TrainingTweet], amount:Int = 10):Unit = rdd.take(amount).foreach(println)

  /**
   * Shows a line graph with sentiment values (y axix) depending on a specific date (x axis)
   */
  def plotData(rdd:RDD[TrainingTweet]):Unit = {
    val sentiments = Training.getSentiments(rdd)
    val dates = Training.getDates(rdd)
    val dataArray = Array.ofDim[Double](2, sentiments.length)

    dataArray(0) = dates      // x values
    dataArray(1) = sentiments // y values

    val dataset = new DefaultXYDataset
    dataset.addSeries("Training Data", dataArray)
    val plotTitle = "Training Data"
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

    println("Please press enter....")
    System.in.read()
    frame.setVisible(false)
    frame.dispose()
  }


  def trainLinearRegression(rdd:RDD[TrainingTweet]):Unit = {

    val relevantData = rdd.map(x => ( Training.downsize(x.date.toLocalDate.toEpochDay), x.date, x.sentiment)) // (Double, LocalDate, Double)

    val df = ss.createDataFrame(relevantData)
      .withColumnRenamed("_1", "features")
      .withColumnRenamed("_2", "dateformats")
      .withColumnRenamed("_3", "sentiments")
      .cache()

    // TODO Hier noch Komplexität überarbeiten
    val transformedData = new VectorAssembler()
      .setInputCols(Array("features"))
      .setOutputCol("transformed_features")
      .transform(df)
      .cache()

    /*
    val scaledData = new StandardScaler()
      .setInputCol("transformed_features")
      .setOutputCol("scaled_features")
      .fit(transformedData)
      .transform(transformedData)
      .cache()

     */

    println("Scaled: ")
    transformedData.columns.foreach(println)
    transformedData.collect().foreach(println)



    val lr = new LinearRegression()
      .setFeaturesCol("transformed_features")
      .setLabelCol("sentiments")

    val model = lr.fit(transformedData)

    println(model.coefficients + "  ---  " + model.intercept)

    val result = model.transform(transformedData)
  }
}

object Training {

  def getSentiments(rdd:RDD[TrainingTweet]):Array[Double] = rdd.map(tweet => tweet.sentiment).collect()

  def getDates(rdd:RDD[TrainingTweet]):Array[Double] =
    rdd.map( tweet => downsize(tweet.date.toLocalDate.toEpochDay) ).collect()

  //Long values of the dates are all over 10000, so they will be downsized for convenience
  def downsize(value:Long):Double = value / 10000.0
}
