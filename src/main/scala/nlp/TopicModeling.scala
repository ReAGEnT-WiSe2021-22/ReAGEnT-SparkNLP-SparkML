package nlp

import nlp.eval.UMassCoherenceModel
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class TopicModeling(spark:SparkSession, data: DataFrame){

  val vectorizerModel = PartyPrediction.getVectorizedDataModel(data.toDF.cache, "cleanText", "features", 2000, 3)
  val vectorizedData = vectorizerModel.transform(data).cache

  private val topics_num = 10
  val ldaModel = getLDAModel(
    vectorizedData,
    topics_num,
    "online",     //Optimizer, currently supported: "em", "online"
    500,
    1/topics_num,
    1/topics_num
  )

  val vocabList = vectorizerModel.vocabulary
  val topicIndices = ldaModel.describeTopics(5)

  //create new LDA model
  def getLDAModel(data:DataFrame, num_topics:Int, optim:String="online", maxIter:Int, alpha:Double, beta:Double) = {
    val lda = new LDA()
      .setK(num_topics)
      .setOptimizer(optim)
      .setMaxIter(maxIter)
      .setDocConcentration(alpha)
      .setTopicConcentration(beta)
      .setFeaturesCol("features")
      .setTopicDistributionCol("topicDistribution")

    val ldaModel = lda.fit(data)
    ldaModel
  }

  //validate model
  def compute_coherence_values(lda:LDAModel) = {
    val topics = lda.describeTopics(5)
    val coherenceModel = new UMassCoherenceModel("features", vectorizedData.select("features"))
    val topicsCoherence = coherenceModel.transform(topics)

    //    topicsCoherence
    //      .select(col("topic"), topicTerms(vectorizerModel.vocabulary, "termIndices").as("terms"), col("topicCoherence"))
    //      .show()

    //    topicsCoherence.select("topicCoherence").foreach(f => println(f))
    //    topicsCoherence.select("topicCoherence")
    //      .agg(avg("topicCoherence")).show()
    topicsCoherence.printSchema()
  }

  def topicTerms(vocabulary: Array[String], termIndicesCol: String): Column = {
    udf((ind: Seq[Int] ) => ind.map(i => vocabulary(i)))
      .apply(col(termIndicesCol))
  }

  //optimize hyperparameters for the model
  def optimizeLDAModel(): Unit ={
    ???
  }

}
