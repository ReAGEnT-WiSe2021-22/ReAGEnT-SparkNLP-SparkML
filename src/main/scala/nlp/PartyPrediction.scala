package nlp

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.clustering.{LDAModel, OnlineLDAOptimizer}
import org.apache.spark.rdd.RDD

//TODO:
//find topics of tweet
//use those in combination with their sentiment as input for NN
//try to predict party with NN-model
class PartyPrediction(spark:SparkSession, data:DataFrame) extends Serializable {

  val vectorizer = new CountVectorizer()
    .setInputCol("cleanText")
    .setOutputCol("features")
    .setVocabSize(10000)
    .setMinDF(3)

  val vectorizerModel = vectorizer.fit(data)
  val countVector = vectorizerModel.transform(data)

  val ldaModel = getLDAModel(
    countVector,
    5,
    "online",     //Optimizer, currently supported: "em", "online"
    100
  )

  val result = ldaModel.transform(countVector)

  import spark.implicits._

  val vocabList = vectorizerModel.vocabulary
  val topicIndices = ldaModel
    .describeTopics(5)
    .map(x => (x.getAs[Seq[Int]]("termIndices"), x.getAs[Seq[Double]]("termWeights")))

  val topics = topicIndices.map { case (terms, weights) =>
    terms.map(vocabList(_)).zip(weights)
  }

  def vectorizeTokens(tokens:DataFrame, inputCol:String, outputCol:String, vocabSize:Int, minDF:Int):DataFrame = {
    val cv = new CountVectorizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setVocabSize(vocabSize)
      .setMinDF(minDF)

    val countVector = cv.fit(tokens).transform(tokens)
    countVector
  }

  def getLDAModel(data:DataFrame, num_topics:Int, optim:String="online", maxIter:Int) = {
    val lda = new LDA()
      .setK(num_topics)
      .setOptimizer(optim)
      .setMaxIter(maxIter)

    val ldaModel = lda.fit(data)

    ldaModel
  }

  def optimizeLDAModel(): Unit ={
    ???
  }
}
