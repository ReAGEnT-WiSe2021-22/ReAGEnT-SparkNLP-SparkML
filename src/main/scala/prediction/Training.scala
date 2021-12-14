package prediction

import org.apache.spark.ml
import org.apache.spark.rdd.RDD
import breeze.plot._
import breeze.linalg._

class Training(data:RDD[TrainingTweet]) {


  def plotting():Unit = {
    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(0.0,1.0)
    p += plot(x, x ^:^ 2.0)
    p += plot(x, x ^:^ 3.0, '.')
    p.xlabel = "x axis"
    p.ylabel = "y axis"
  }

}
