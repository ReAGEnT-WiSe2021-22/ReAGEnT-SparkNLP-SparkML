package prediction

import org.apache.spark.ml
import org.apache.spark.rdd.RDD
import breeze.plot._
import breeze.linalg._

import org.jfree.data.statistics.Regression

class Training(data:RDD[TrainingTweet]) {


  def plotData0():Unit = {
    //val sorted = data.sortBy(x => x.date.toEpochDay).cache()
    //val dates = sorted.map(x => x.date.toEpochDay.toDouble).collect()
    //val sentiments = sorted.map(x => x.sentiment).collect()

    //val x = new DenseVector[Double](dates)
    //val y = new DenseVector[Double](sentiments)

    val f = Figure()
    val p = f.subplot(0)

    //p += plot(x, y)

    p.xlabel = "Dates"
    p.ylabel = "Sentiments"
  }


  def plotData():Unit = {

  }

}
