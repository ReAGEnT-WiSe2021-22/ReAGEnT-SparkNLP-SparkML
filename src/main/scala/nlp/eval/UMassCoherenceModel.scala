package nlp.eval

import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}

class UMassCoherenceModel(override val uid: String,
                          val documentTfCol: String,
                          val docTerms: Dataset[_]) extends Model[UMassCoherenceModel] {

  def this(documentTfCol: String, docTerms: Dataset[_]) = this(Identifiable.randomUID("topic-coherence-model"), documentTfCol, docTerms)

  //LDA topic description has constant field names
  private val topicIdCol = "topic"
  private val topicTermsCol = "termIndices"

  override def copy(extra: ParamMap): UMassCoherenceModel = defaultCopy(extra)

  override def transform(topicDescription: Dataset[_]): DataFrame = {
    val topicTermPermutations = topicDescription.select(col(topicIdCol), topicTermsPermutationsCol(topicTermsCol).as(topicTermsCol))
    val documentToTopic = docTerms.select(documentTfCol).crossJoin(topicTermPermutations)

    val termPairOccurrencesPerDoc = documentToTopic.select(col(topicIdCol),
      pairTermsOccurrencesPerDocumentCol(topicTermsCol).as(topicTermsCol))

    val coherence = termPairOccurrencesPerDoc.groupBy(col(topicIdCol))
      .agg(topicSumAggregation(topicTermsCol).as(topicTermsCol))
      .select(col(topicIdCol), topicCoherenceCol(topicTermsCol).as("topicCoherence"))

    topicDescription.join(coherence, topicIdCol)
  }

  override def transformSchema(schema: StructType): StructType = schema.add(name = "topicCoherence", dataType = DataTypes.DoubleType, nullable = false)

  private def topicTermsPermutationsCol(termIndicesCol: String): Column = {
    udf(topicTermsPermutationsTransform)
      .apply(col(termIndicesCol))
  }

  // T = [W_1,...,W_n] => [(W_i, W_j)] forall i < j
  private def topicTermsPermutationsTransform: Array[Int] => Array[(Int, Int)] = { topicTermsIndices =>
    assert(topicTermsIndices.length >= 2)
    topicTermsIndices.slice(0, topicTermsIndices.length - 2).zipWithIndex.flatMap { p =>
      topicTermsIndices.slice(p._2 + 1, topicTermsIndices.length - 1)
        .map(term => (p._1, term))
    }
  }

  def pairTermsOccurrencesPerDocumentCol(termPairsCol: String): Column = {
    udf(pairTermsOccurrencesPerDocumentTransform).apply(col(documentTfCol), col(termPairsCol))
  }

  def pairTermsOccurrencesPerDocumentTransform: (SparseVector, Array[(Int, Int)]) => Array[(Boolean, Boolean)] = { (docTerms, topics) =>
    topics.map { p =>
      val firstTermPresent = docTerms(p._1) > 0
      val bothTermPresent = firstTermPresent && (docTerms(p._2) > 0)
      (firstTermPresent, bothTermPresent)
    }
  }

  private def topicSumAggregation(termPairsCol: String): Column =
    udaf(TopicTermPairOccurrencesPerCorpusAggregator).apply(col(termPairsCol))

  private def topicCoherenceCol(pairTermsOccurrencesPerCorpusCol: String): Column = {
    udf(topicCoherenceTransform)
      .apply(col(pairTermsOccurrencesPerCorpusCol))
  }

  private def topicCoherenceTransform: Array[(Long, Long)] => Double = { topicStats =>
    topicStats.map {
      case (x, y) => Math.log((y + 1.0) / x) / Math.log(2)
    }.sum
  }

}

