package nlp

import nlp.eval.UMassCoherenceModel
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col, udf}

//TODO:
//find topics of tweet
//use those in combination with their sentiment as input for NN
//try to predict party with NN-model
class PartyPrediction(spark:SparkSession, data:DataFrame) extends Serializable {

  import spark.implicits._
  val vectorizerModel = PartyPrediction.getVectorizedDataModel(data, "cleanText", "features", 2000, 3)
  val vectorized_data = vectorizerModel.transform(data).cache

  //create model

  //evaluate model

  //train model

}
object PartyPrediction {
  def getVectorizedDataModel(data:DataFrame, inputCol:String, outputCol:String, vocabSize:Int, minOccur:Int):CountVectorizerModel ={
    val vectorizer = new CountVectorizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setVocabSize(vocabSize)
      .setMinDF(minOccur)

    vectorizer.fit(data)
  }
}


