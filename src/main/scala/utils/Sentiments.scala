package utils

/**
 * Will maybe be used for calculating sentiment values
 * @param sentiFile file with words and sentimen values
 */
class Sentiments(sentiFile: String) {

  val sentiments: Map[String, Int] = getSentiments(sentiFile)

  def getSentiments(filename: String): Map[String, Int] = {
    val url = getClass.getResource("/" + filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    val result: Map[String, Int] = (for (row <- iter) yield {
      val seg = row.split("\t"); (seg(0) -> seg(1).toInt)
    }).toMap
    src.close()
    result
  }

  def analyseSentiments(tweet: List[(Int, List[String])]): List[(Int, Double)] = {
    val sentiments = getSentiments("AFINN-111.txt")
    val filtered = tweet.map(x => ( x._1, x._2.filter(y => sentiments.contains(y)) ))
    filtered.foreach(println)

    def redFun(list:List[(Int, Double)], elem:(Int, List[String])):List[(Int, Double)] = {
      val changed = elem._2.filter(x => sentiments.contains(x)).map(y => sentiments.getOrElse(y,0)) //List[Int] (Sentiment-Werte)
      if (changed.nonEmpty) list++List(( elem._1, (changed.sum:Double)/changed.size ))
      else list++List((elem._1, 0.0))
    }

    filtered.foldLeft(List():List[(Int, Double)])(redFun)
  }
}
