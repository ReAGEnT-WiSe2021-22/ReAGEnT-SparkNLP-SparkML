package nlp.eval

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

case class TopicTermPairOccurrences(pairs: Array[(Long, Long)])

object TopicTermPairOccurrencesPerCorpusAggregator extends Aggregator[Array[(Boolean, Boolean)], TopicTermPairOccurrences, Array[(Long, Long)]] {

  override def zero: TopicTermPairOccurrences = TopicTermPairOccurrences(Array())

  override def reduce(b: TopicTermPairOccurrences, a: Array[(Boolean, Boolean)]): TopicTermPairOccurrences = {
    sumTopicsWithBool(b.pairs, a)
  }

  override def merge(b1: TopicTermPairOccurrences, b2: TopicTermPairOccurrences): TopicTermPairOccurrences = {
    sumTopics(b1.pairs, b2.pairs)
  }

  override def finish(reduction: TopicTermPairOccurrences): Array[(Long, Long)] = reduction.pairs

  override def bufferEncoder: Encoder[TopicTermPairOccurrences] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[(Long, Long)]] = ExpressionEncoder()

  private def sumTopics(topics1: Array[(Long, Long)], topics2: Array[(Long, Long)]): TopicTermPairOccurrences = {
    val sum = (topics1, topics2) match {
      case (Array(), b) => b
      case (a, Array()) => a
      case _ => topics1.zip(topics2).map {
        case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
      }
    }
    TopicTermPairOccurrences(sum)
  }

   private def sumTopicsWithBool(topics1: Array[(Long, Long)], topics2: Array[(Boolean, Boolean)]): TopicTermPairOccurrences = {
    val converted = topics2.map {
      case (x, y) => (boolToInt(x), boolToInt(y))
    }
    sumTopics(topics1, converted)
  }

  private def boolToInt(b: Boolean): Long = if (b) 1 else 0


}
