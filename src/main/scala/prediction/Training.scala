package prediction

import org.apache.spark.ml
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jfree.data.xy.DefaultXYDataset
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart, plot}
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
      .groupBy(x => x.date.toLocalDate.toEpochDay)
      .map(tweetsWithSameCreationDate => {
        val head = tweetsWithSameCreationDate._2.head
        val sentiments = tweetsWithSameCreationDate._2.map(_.sentiment)
        TrainingTweet(head.party, head.text, head.date, sentiments.sum/sentiments.size)
      })
      .sortBy(x => x.date.toLocalDate.toEpochDay)
  }


  def getSentiments(rdd:RDD[TrainingTweet]):Array[Double] = rdd.map(tweet => tweet.sentiment).collect()

  def getDates(rdd:RDD[TrainingTweet]):Array[Double] = rdd.map(tweet => tweet.date.toLocalDate.toEpochDay.toDouble).collect()


  /**
   * @param amount How many tweets should be printed
   */
  def printData(rdd:RDD[TrainingTweet], amount:Int = 10):Unit = rdd.take(amount).foreach(println)


  /**
   * Shows a line graph with sentiment values (y axix) depending on a specific date (x axis)
   */
  def plotData(rdd:RDD[TrainingTweet]):Unit = {
    val sentiments = getSentiments(rdd)
    val dates = getDates(rdd)
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

    val frame:JFrame = new JFrame("Verlauf der sentiments Test")
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
    val features = rdd.map(x => (x.date.toLocalDate.toEpochDay, x.sentiment))
    val df = ss.createDataFrame(features).cache()

    df.columns.foreach(println)
    // columns : "party", "text", "date", "sentiment"
    // columns : "_1", "_2"

    val modelData = new VectorAssembler()
      .setInputCols(Array("_1"))
      .setOutputCol("features")
      .transform(df)
      .cache()

    /*
    TODO
    - Feature Scaling?
    - Passen die Columns?
    - Rename die columns (nicht _1 & _2)
    - Formel wählen für das Training, z. B. y = x^3+x
    - Passenden Algorithmus wählen
    */


    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("_2")
      .setMaxIter(10)

    val model = lr.fit(modelData)

    println(model.coefficients + "  ---  " + model.intercept)
  }


}
